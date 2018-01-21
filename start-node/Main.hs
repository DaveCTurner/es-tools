{-# LANGUAGE TupleSections #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import Text.Printf
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import System.Posix.Signals
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Data.Conduit
import Data.Conduit.Binary hiding (mapM_, take)
import Data.Conduit.Process
import Data.Maybe
import Data.Streaming.Process
import Data.Time
import Data.Time.ISO8601
import System.Directory
import System.Environment
import System.Exit
import System.FilePath
import System.IO
import System.Process
import System.Process.Internals
import qualified Data.ByteString as B
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

terminateStreamingProcess :: StreamingProcessHandle -> IO ()
terminateStreamingProcess = terminateProcess . streamingProcessHandleRaw

mkEnv :: [(String, Maybe String)] -> Maybe [(String, String)]
mkEnv = Just . mapMaybe (\(k, mv) -> fmap (k,) mv)

data CurrentRun = CurrentRun
  { crName :: String
  , crWorkingDirectory :: FilePath
  , crWriteLog :: String -> IO ()
  }

class CanLog a where
  writeLog :: a -> String -> IO ()

instance CanLog CurrentRun where
  writeLog = crWriteLog

withCurrentRun :: (CurrentRun -> IO a) -> IO a
withCurrentRun go = do
  runName <- formatTime defaultTimeLocale "%Y-%m-%d--%H-%M-%s.%q" <$> getCurrentTime
  cwd <- getCurrentDirectory
  let workingDirectory = cwd </> "output" </> runName
  createDirectoryIfMissing True workingDirectory
  withFile (workingDirectory </> "run.log") WriteMode $ \hLog -> do

    logLock <- newMVar ()

    let writeLogCurrentRun msg = withMVar logLock $ \() -> do
          now <- formatISO8601Micros <$> getCurrentTime
          let fullMsg = "[" ++ now ++ "] " ++ msg
          putStrLn fullMsg
          hPutStrLn hLog fullMsg

    writeLogCurrentRun $ printf "Starting run with working directory: " ++ workingDirectory
    go CurrentRun
      { crName             = runName
      , crWorkingDirectory = workingDirectory
      , crWriteLog         = writeLogCurrentRun
      }

data NodeConfig = NodeConfig
  { ncCurrentRun           :: CurrentRun
  , ncName                 :: String
  , ncIsMasterEligibleNode :: Bool
  , ncIsDataNode           :: Bool
  , ncHttpPort             :: Int
  , ncPublishPort          :: Int
  , ncJavaHome             :: Maybe String
  , ncUnicastHostPorts     :: [Int]
  }

instance CanLog NodeConfig where
  writeLog nc = writeLog (ncCurrentRun nc) . printf "[%-9s] %s" (ncName nc)

stdoutPath :: NodeConfig -> FilePath
stdoutPath nc = nodeWorkingDirectory nc </> "stdout.log"

stderrPath :: NodeConfig -> FilePath
stderrPath nc = nodeWorkingDirectory nc </> "stderr.log"

nodeWorkingDirectory :: NodeConfig -> FilePath
nodeWorkingDirectory nc = crWorkingDirectory (ncCurrentRun nc) </> ncName nc

configDirectory :: NodeConfig -> FilePath
configDirectory nc = nodeWorkingDirectory nc </> "config"

sourceConfig :: Monad m => NodeConfig -> Producer m B.ByteString
sourceConfig nc = mapM_ yieldString
  [ "cluster.name: " ++ crName (ncCurrentRun nc)
  , "node.name: " ++ ncName nc
  , "discovery.zen.minimum_master_nodes: 2"
  , "node.data: " ++ if ncIsDataNode nc then "true" else "false"
  , "node.master: " ++ if ncIsMasterEligibleNode nc then "true" else "false"
  , "path.data: " ++ (nodeWorkingDirectory nc </> "data")
  , "path.logs: " ++ (nodeWorkingDirectory nc </> "logs")
  , "network.host: 127.0.0.1"
  , "http.port: " ++ show (ncHttpPort nc)
  , "transport.tcp.port: " ++ show (ncPublishPort nc)
  , "discovery.zen.ping.unicast.hosts: " ++ show [ "127.0.0.1:" ++ show p | p <- ncUnicastHostPorts nc]
  ]

yieldString :: Monad m => String -> Producer m B.ByteString
yieldString = yield . T.encodeUtf8 . T.pack . (++ "\n")

sourceJvmOptions :: Monad m => Producer m B.ByteString
sourceJvmOptions = mapM_ yieldString
  [ "-Xms1g"
  , "-Xmx1g"
  , "-XX:+UseConcMarkSweepGC"
  , "-XX:CMSInitiatingOccupancyFraction=75"
  , "-XX:+UseCMSInitiatingOccupancyOnly"
  , "-XX:+AlwaysPreTouch"
  , "-server"
  , "-Xss1m"
  , "-Djava.awt.headless=true"
  , "-Dfile.encoding=UTF-8"
  , "-Djna.nosys=true"
  , "-XX:-OmitStackTraceInFastThrow"
  , "-Dio.netty.noUnsafe=true"
  , "-Dio.netty.noKeySetOptimization=true"
  , "-Dio.netty.recycler.maxCapacityPerThread=0"
  , "-Dlog4j.shutdownHookEnabled=false"
  , "-Dlog4j2.disable.jmx=true"
  , "-XX:+HeapDumpOnOutOfMemoryError"
  ]

makeConfig :: NodeConfig -> IO ()
makeConfig nc = do
  writeLog nc "makeConfig"

  createDirectoryIfMissing True $ configDirectory nc

  runResourceT $ runConduit
     $  sourceFile "/Users/davidturner/stack-6.1.1/elasticsearch-6.1.1/config/log4j2.properties"
    =$= sinkFile   (configDirectory nc </> "log4j2.properties")

  runResourceT $ runConduit
     $  sourceJvmOptions
    =$= sinkFile   (configDirectory nc </> "jvm.options")

  runResourceT $ runConduit
     $  sourceConfig nc
    =$= sinkFile   (configDirectory nc </> "elasticsearch.yml")

writeToConsole :: MonadIO m => ConduitM B.ByteString B.ByteString m ()
writeToConsole = awaitForever $ \bs -> do
  liftIO $ B.putStr bs
  yield bs

checkStarted :: MonadIO m => IO () -> ConduitM B.ByteString B.ByteString m ()
checkStarted onStarted = awaitForever $ \bs -> do
  when ("started" `B.isInfixOf` bs) $ do
    liftIO onStarted
    yield bs
    awaitForever yield

  yield bs

data ElasticsearchNode = ElasticsearchNode
  { esnConfig    :: NodeConfig
  , esnHandle    :: StreamingProcessHandle
  , esnIsStarted :: STM Bool
  , esnThread    :: Async ExitCode
  }

instance CanLog ElasticsearchNode where
  writeLog = writeLog . esnConfig

runNode :: NodeConfig -> IO ElasticsearchNode
runNode nodeConfig = do

  writeLog nodeConfig "runNode"

  makeConfig nodeConfig

  (  ClosedStream
   , (sourceStdout, closeStdout)
   , (sourceStderr, closeStderr)
   , sph) <- streamingProcess $
        (proc "/Users/davidturner/stack-6.1.1/elasticsearch-6.1.1/bin/elasticsearch" [])
        { cwd = Just $ crWorkingDirectory $ ncCurrentRun nodeConfig
        , env = mkEnv $
            [("ES_PATH_CONF", Just $ configDirectory nodeConfig)
            ,("JAVA_HOME", ncJavaHome nodeConfig)]
        }

  withProcessHandle (streamingProcessHandleRaw sph) $ \case
    OpenHandle pid -> writeLog nodeConfig $ "started with PID " ++ show pid
    _              -> writeLog nodeConfig $ "started but not OpenHandle"

  saidStartedVar <- newTVarIO False

  nodeThread <- async $
    withFile (stdoutPath nodeConfig) AppendMode $ \stdoutLog ->
    withFile (stderrPath nodeConfig) AppendMode $ \stderrLog -> do

    let concurrentConduit = Concurrently . runConduit

        onStarted = do
          writeLog nodeConfig "onStarted"
          atomically $ writeTVar saidStartedVar True

        terminateAndLog = do
          writeLog nodeConfig "terminateAndLog"
          terminateProcess $ streamingProcessHandleRaw sph

    ((), ()) <- runConcurrently ((,)
        <$> concurrentConduit (sourceStdout =$= writeToConsole =$= checkStarted onStarted
                                                               =$= sinkHandle stdoutLog)
        <*> concurrentConduit (sourceStderr =$= writeToConsole =$= sinkHandle stderrLog))
      `finally`     (closeStdout >> closeStderr)
      `onException` terminateAndLog

    ec <- waitForStreamingProcess sph
    writeLog nodeConfig $ "exited: " ++ show ec
    return ec

  return $ ElasticsearchNode
    { esnConfig    = nodeConfig
    , esnHandle    = sph
    , esnIsStarted = readTVar saidStartedVar
    , esnThread    = nodeThread
    }

signalNode :: ElasticsearchNode -> Signal -> IO ()
signalNode n@ElasticsearchNode{..} signal = withProcessHandle ph $ \case
  OpenHandle pid -> do
    writeLog n $ "sending signal " ++ show signal ++ " to PID " ++ show pid
    signalProcess signal pid
  _ ->
    writeLog n $ "not sending signal " ++ show signal ++ " as handle is not OpenHandle"
  where ph = streamingProcessHandleRaw esnHandle

awaitStarted :: ElasticsearchNode -> STM Bool
awaitStarted ElasticsearchNode{..} = saidStarted `orElse` threadExited
  where
  saidStarted = do
    isStarted <- esnIsStarted
    if isStarted then return True else retry

  threadExited = waitSTM esnThread >> return False

awaitExit :: ElasticsearchNode -> STM ExitCode
awaitExit ElasticsearchNode{..} = waitSTM esnThread

main :: IO ()
main = withCurrentRun $ \currentRun -> do

  javaHome <- lookupEnv "JAVA_HOME"

  let nodeConfigs
        = [ NodeConfig
            { ncCurrentRun           = currentRun
            , ncName                 = (if isMaster then "master-" else "data-") ++ show nodeIndex
            , ncIsMasterEligibleNode = isMaster
            , ncIsDataNode           = not isMaster
            , ncHttpPort             = 19200 + nodeIndex
            , ncPublishPort          = 19300 + nodeIndex
            , ncJavaHome             = javaHome
            , ncUnicastHostPorts     = [ncPublishPort nc | nc <- nodeConfigs, ncIsMasterEligibleNode nc]
            }
          | nodeIndex <- [1..6]
          , let isMaster = nodeIndex <= 3
          ]

  nodes <- mapM runNode nodeConfigs
  let faultyNodes = take 2 nodes

  startedFlags <- forM nodes $ \n -> do
    result <- atomically $ awaitStarted n
    writeLog n $ if result then "started successfully" else "did not start successfully"
    return result

  if not (and startedFlags)
    then writeLog currentRun "not all nodes started successfully"
    else do

      threadDelay 10000000
      writeLog currentRun "pausing some nodes"
      forM_ faultyNodes $ \n -> signalNode n sigTSTP

      threadDelay 100000000
      writeLog currentRun "resuming paused nodes"
      forM_ faultyNodes $ \n -> signalNode n sigCONT

      writeLog currentRun "killing some nodes"
      threadDelay 10000000
      forM_ faultyNodes $ \n -> do
        signalNode n sigKILL
        atomically $ awaitExit n

      writeLog currentRun "terminating all nodes"
      forM_ nodes $ \n -> do
        signalNode n sigTERM
        atomically $ awaitExit n
        writeLog n "terminated"

  writeLog currentRun "finished"
