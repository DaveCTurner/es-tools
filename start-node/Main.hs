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

configDirectory :: CurrentRun -> FilePath
configDirectory cr = crWorkingDirectory cr </> "config"

stdoutPath :: CurrentRun -> FilePath
stdoutPath cr = crWorkingDirectory cr </> "stdout.log"

stderrPath :: CurrentRun -> FilePath
stderrPath cr = crWorkingDirectory cr </> "stderr.log"

sourceConfig :: Monad m => CurrentRun -> Producer m B.ByteString
sourceConfig cr = mapM_ yieldString 
  [ "cluster.name: " ++ crName cr
  , "node.name: data-1"
  , "discovery.zen.minimum_master_nodes: 1"
  , "node.master: true"
  , "node.data: true"
  , "path.data: " ++ (crWorkingDirectory cr </> "data")
  , "path.logs: " ++ (crWorkingDirectory cr </> "logs")
  , "network.host: 127.0.0.1"
  , "http.port: 9201"
  , "discovery.zen.ping.unicast.hosts: []"
  ]
  where yieldString :: Monad m => String -> Producer m B.ByteString
        yieldString = yield . T.encodeUtf8 . T.pack . (++ "\n")

makeConfig :: CurrentRun -> IO ()
makeConfig cr = do
  createDirectoryIfMissing True $ configDirectory cr

  runResourceT $ runConduit
     $  sourceFile "/Users/davidturner/stack-6.1.1/elasticsearch-6.1.1/config/log4j2.properties"
    =$= sinkFile   (configDirectory cr </> "log4j2.properties")

  runResourceT $ runConduit
     $  sourceFile "/Users/davidturner/stack-6.1.1/elasticsearch-6.1.1/config/jvm.options"
    =$= sinkFile   (configDirectory cr </> "jvm.options")

  runResourceT $ runConduit
     $  sourceConfig cr
    =$= writeToConsole
    =$= sinkFile   (configDirectory cr </> "elasticsearch.yml")

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

  javaHome <- lookupEnv "JAVA_HOME"

  makeConfig currentRun

  withFile   (stdoutPath currentRun) WriteMode $ \stdoutLog -> 
    withFile (stderrPath currentRun) WriteMode $ \stderrLog -> do

    saidStartedVar <- newEmptyMVar

    let cp0 = proc "/Users/davidturner/stack-6.1.1/elasticsearch-6.1.1/bin/elasticsearch" []
        cp = cp0
          { cwd = Just $ crWorkingDirectory currentRun
          , env = mkEnv $
              [("ES_PATH_CONF", Just $ configDirectory currentRun)
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

    return ()
