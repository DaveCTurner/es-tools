{-# LANGUAGE LambdaCase #-}

module DockerNetwork 
  ( DockerNetwork
  , dockerNetworkId
  , withDockerNetwork
  , withTcpdump
  ) where

import LogContext
import System.Exit
import Control.Exception
import Control.Concurrent (threadDelay)
import System.Process
import System.Process.Internals
import Process
import Data.Streaming.Process
import System.Posix.Signals
import System.Timeout

newtype DockerNetwork = DockerNetwork String
  deriving (Show, Eq)

dockerNetworkId :: DockerNetwork -> String
dockerNetworkId (DockerNetwork s) = s

dockerNetworkCreate :: LogContext lc => lc -> IO DockerNetwork
dockerNetworkCreate logContext = DockerNetwork <$> do

  let networkName = "elasticnet"

      args = ["network", "create", "--driver=bridge", "--subnet=10.10.10.0/24", "--ip-range=10.10.10.0/24"
             ,"--opt", "com.docker.network.bridge.name=" ++ networkName, networkName]

  writeLog logContext $ "dockerNetworkCreate: running docker " ++ unwords args

  (ec, stdoutContent, stderrContent) <- readProcessWithExitCode "docker" args ""

  if null stderrContent && ec == ExitSuccess
    then return $ filter (> ' ') stdoutContent
    else do
      writeLog logContext $ "dockerNetworkCreate: docker " ++ unwords args ++ " exited with " ++ show ec ++ " yielding '" ++ stderrContent ++ "'"
      error "docker network create failed"

dockerNetworkRemove :: LogContext lc => lc -> DockerNetwork-> IO ()
dockerNetworkRemove logContext dockerNetwork = do
  threadDelay 1000000
  callProcessNoThrow logContext "docker" [ "network", "rm", dockerNetworkId dockerNetwork ]

withDockerNetwork :: LogContext lc => lc -> (DockerNetwork -> IO a) -> IO a
withDockerNetwork logContext go = bracket
  (dockerNetworkCreate logContext)
  (dockerNetworkRemove logContext) $ \dockerNetwork -> do
    writeLog logContext $ "Using docker network " ++ dockerNetworkId dockerNetwork
    go dockerNetwork

withTcpdump :: LogContext lc => lc -> String -> IO a -> IO a
withTcpdump logContext fileName go = bracket setup teardown $ const go
  where
  cmd = "sudo tcpdump -i elasticnet -s65535 -w" ++ fileName
  setup = do
    writeLog logContext $ "Starting " ++ cmd
    (Inherited, Inherited, Inherited, sph) <- streamingProcess (shell cmd)
    return sph

  teardown sph = do
    writeLog logContext $ "Killing tcpdump"
    maybeExitCode <- timeout 5000000 $ do
      withProcessHandle (streamingProcessHandleRaw sph) $ \case
        OpenHandle pid -> do
          writeLog logContext $ "Sending SIGTERM to pid " ++ show pid
          signalProcess sigTERM pid
        _ -> return ()
      waitForStreamingProcess sph
    writeLog logContext $ "Killing tcpdump resulted in " ++ show maybeExitCode
      


