module DockerNetwork 
  ( DockerNetwork
  , dockerNetworkId
  , withDockerNetwork
  ) where

import LogContext
import System.Exit
import Control.Exception
import Control.Concurrent (threadDelay)
import System.Process
import Process

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