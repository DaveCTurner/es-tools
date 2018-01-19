{-# LANGUAGE TupleSections #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}

module Main where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Exception
import System.Posix.Signals
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Data.Conduit
import Data.Conduit.Binary hiding (mapM_)
import Data.Conduit.Process
import Data.Maybe
import Data.Streaming.Process
import Data.Time
import System.Directory
import System.Environment
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
  }

getCurrentRun :: IO CurrentRun
getCurrentRun = do
  runName <- formatTime defaultTimeLocale "%Y-%m-%d--%H-%M-%s.%q" <$> getCurrentTime
  cwd <- getCurrentDirectory
  let workingDirectory = cwd </> "output" </> runName
  createDirectoryIfMissing True workingDirectory
  putStrLn $ "Working directory: " ++ workingDirectory
  return CurrentRun
    { crName = runName
    , crWorkingDirectory = workingDirectory
    }

data NodeConfig = NodeConfig
  { ncCurrentRun           :: CurrentRun
  , ncName                 :: String
  , ncIsMasterEligibleNode :: Bool
  , ncIsDataNode           :: Bool
  , ncHttpPort             :: Int
  , ncPublishPort          :: Int
  }

stdoutPath :: NodeConfig -> FilePath
stdoutPath nc = nodeWorkingDirectory nc </> "stdout.log"

stderrPath :: NodeConfig -> FilePath
stderrPath nc = nodeWorkingDirectory nc </> "stderr.log"

nodeWorkingDirectory :: NodeConfig -> FilePath
nodeWorkingDirectory nc = crWorkingDirectory (ncCurrentRun nc) </> ncName nc

configDirectory :: NodeConfig -> FilePath
configDirectory nc = nodeWorkingDirectory nc </> "config"

sourceConfig :: Monad m => [Int] -> NodeConfig -> Producer m B.ByteString
sourceConfig otherNodePorts nc = mapM_ yieldString
  [ "cluster.name: " ++ crName (ncCurrentRun nc)
  , "node.name: " ++ ncName nc
  , "discovery.zen.minimum_master_nodes: 2"
  , "node.master: " ++ if ncIsDataNode nc then "true" else "false"
  , "node.data: " ++ if ncIsMasterEligibleNode nc then "true" else "false"
  , "path.data: " ++ (nodeWorkingDirectory nc </> "data")
  , "path.logs: " ++ (nodeWorkingDirectory nc </> "logs")
  , "network.host: 127.0.0.1"
  , "http.port: " ++ show (ncHttpPort nc)
  , "transport.tcp.port: " ++ show (ncPublishPort nc)
  , "discovery.zen.ping.unicast.hosts: " ++ show [ "127.0.0.1:" ++ show p | p <- otherNodePorts ]
  ]
  where yieldString :: Monad m => String -> Producer m B.ByteString
        yieldString = yield . T.encodeUtf8 . T.pack . (++ "\n")

makeConfig :: [Int] -> NodeConfig -> IO ()
makeConfig otherNodePorts nc = do
  createDirectoryIfMissing True $ configDirectory nc

  runResourceT $ runConduit
     $  sourceFile "/Users/davidturner/stack-6.1.1/elasticsearch-6.1.1/config/log4j2.properties"
    =$= sinkFile   (configDirectory nc </> "log4j2.properties")

  runResourceT $ runConduit
     $  sourceFile "/Users/davidturner/stack-6.1.1/elasticsearch-6.1.1/config/jvm.options"
    =$= sinkFile   (configDirectory nc </> "jvm.options")

  runResourceT $ runConduit
     $  sourceConfig otherNodePorts nc
    =$= writeToConsole
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

main :: IO ()
main = do

  currentRun <- getCurrentRun
  let nodeConfigs
        = [ NodeConfig
            { ncCurrentRun           = currentRun
            , ncName                 = (if isMaster then "master-" else "data-") ++ show nodeIndex
            , ncIsMasterEligibleNode = isMaster
            , ncIsDataNode           = not isMaster
            , ncHttpPort             = 9200 + nodeIndex
            , ncPublishPort          = 9300 + nodeIndex
            }
          | nodeIndex <- [1..6]
          , let isMaster = nodeIndex <= 3
          ]

  javaHome <- lookupEnv "JAVA_HOME"

  nodeManagers <- forM nodeConfigs $ \nodeConfig -> do

    makeConfig (map ncPublishPort nodeConfigs) nodeConfig

    async $
      withFile (stdoutPath nodeConfig) WriteMode $ \stdoutLog ->
      withFile (stderrPath nodeConfig) WriteMode $ \stderrLog -> do

      saidStartedVar <- newEmptyMVar

      let cp0 = proc "/Users/davidturner/stack-6.1.1/elasticsearch-6.1.1/bin/elasticsearch" []
          cp = cp0
            { cwd = Just $ crWorkingDirectory currentRun
            , env = mkEnv $
                [("ES_PATH_CONF", Just $ configDirectory nodeConfig)
                ,("JAVA_HOME", javaHome)]
            }

          consumerStdout :: Consumer B.ByteString IO ()
          consumerStdout = writeToConsole =$= checkStarted (putMVar saidStartedVar ())
                                          =$= sinkHandle stdoutLog

          consumerStderr :: Consumer B.ByteString IO ()
          consumerStderr = writeToConsole =$= sinkHandle stderrLog

      (  ClosedStream
       , (sourceStdout, closeStdout)
       , (sourceStderr, closeStderr)
       , sph) <- streamingProcess cp

      pid <- withProcessHandle (streamingProcessHandleRaw sph) $ \case
        OpenHandle pid -> return pid
        ClosedHandle exitCode -> error $ "Process already exited with code " ++ show exitCode

      let onStartedNode = do
            readMVar saidStartedVar
            putStrLn $ "node is started with pid " ++ show pid
            threadDelay 10000000
            putStrLn $ "sending SIGTSTP to pid " ++ show pid
            signalProcess sigTSTP pid
            threadDelay 10000000
            putStrLn $ "sending SIGCONT to pid " ++ show pid
            signalProcess sigCONT pid
            threadDelay 10000000
            putStrLn $ "sending SIGKILL to pid " ++ show pid
            signalProcess sigKILL pid

      withAsync onStartedNode $ \_ -> do
        ((), ()) <-
          runConcurrently (
            (,)
            <$> Concurrently (sourceStdout  $$ consumerStdout)
            <*> Concurrently (sourceStderr  $$ consumerStderr))
          `finally` (closeStdout >> closeStderr)
          `onException` terminateStreamingProcess sph
        ec <- waitForStreamingProcess sph
        print $ "Exit code: " ++ show ec

  mapM_ wait nodeManagers
