{-# LANGUAGE TupleSections #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import Control.Lens hiding ((.=))
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Data.Aeson
import Data.Aeson.Lens
import Data.Conduit
import Data.Conduit.Binary hiding (mapM_, take, head)
import Data.Conduit.Process
import Data.Hashable
import Data.Maybe
import Data.Streaming.Process
import Data.Time
import Data.Time.ISO8601
import Network.HTTP.Client
import Network.HTTP.Types.Header
import System.Directory
import System.Environment
import System.Exit
import System.FilePath
import System.IO
import System.Posix.Signals
import System.Process
import System.Process.Internals
import Text.Printf
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

terminateStreamingProcess :: StreamingProcessHandle -> IO ()
terminateStreamingProcess = terminateProcess . streamingProcessHandleRaw

mkEnv :: [(String, Maybe String)] -> Maybe [(String, String)]
mkEnv = Just . mapMaybe (\(k, mv) -> fmap (k,) mv)

data CurrentRun = CurrentRun
  { crName             :: String
  , crWorkingDirectory :: FilePath
  , crWriteLog         :: String -> IO ()
  , crManager          :: Manager
  , crDockerNetwork    :: DockerNetwork
  }

class CanLog a where
  writeLog :: a -> String -> IO ()

instance CanLog CurrentRun where
  writeLog = crWriteLog

newtype DockerNetwork = DockerNetwork { _unDockerNetwork :: String }
  deriving (Show, Eq)

dockerNetworkCreate :: String -> IO DockerNetwork
dockerNetworkCreate networkName = DockerNetwork <$> do

  let args = ["network", "create", "--driver=bridge", "--subnet=10.10.10.0/24", "--ip-range=10.10.10.0/24"
             ,"--opt", "com.docker.network.bridge.name=" ++ networkName, networkName]

  putStrLn $ unwords args

  (ec, stdout, stderr) <- readProcessWithExitCode "docker" args ""

  if null stderr && ec == ExitSuccess
    then return $ filter (> ' ') stdout
    else error ("docker network create failed: " ++ stderr)

dockerNetworkRemove :: DockerNetwork-> IO ()
dockerNetworkRemove dockerNetwork = callProcess "docker" [ "network", "rm", _unDockerNetwork dockerNetwork ]

withCurrentRun :: (CurrentRun -> IO a) -> IO a
withCurrentRun go = do
  runName <- formatTime defaultTimeLocale "%Y-%m-%d--%H-%M-%s.%q" <$> getCurrentTime
  cwd <- getCurrentDirectory
  let workingDirectory = cwd </> "output" </> runName
      networkName = "elasticnet"

  createElasticDirectory workingDirectory

  withFile (workingDirectory </> "run.log") WriteMode $ \hLog ->
    bracket (dockerNetworkCreate networkName) dockerNetworkRemove $ \dockerNetwork -> do

    putStrLn $ "Using docker network " ++ show dockerNetwork

    logLock <- newMVar ()

    let writeLogCurrentRun msg = withMVar logLock $ \() -> do
          now <- formatISO8601Micros <$> getCurrentTime
          let fullMsg = "[" ++ now ++ "] " ++ msg
          putStrLn fullMsg
          hPutStrLn hLog fullMsg

    manager <- newManager defaultManagerSettings

    writeLogCurrentRun $ printf "Starting run with working directory: " ++ workingDirectory
    go CurrentRun
      { crName             = runName
      , crWorkingDirectory = workingDirectory
      , crWriteLog         = writeLogCurrentRun
      , crManager          = manager
      , crDockerNetwork    = dockerNetwork
      }

data NodeConfig = NodeConfig
  { ncCurrentRun           :: CurrentRun
  , ncName                 :: String
  , ncIsMasterEligibleNode :: Bool
  , ncIsDataNode           :: Bool
  , ncHttpPort             :: Int
  , ncPublishPort          :: Int
  , ncJavaHome             :: Maybe String
  , ncBindHost             :: String
  , ncUnicastHosts         :: [String]
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
  , "network.host: " ++ ncBindHost nc
  , "http.port: " ++ show (ncHttpPort nc)
  , "transport.tcp.port: " ++ show (ncPublishPort nc)
  , "discovery.zen.ping.unicast.hosts: " ++ show (ncUnicastHosts nc)
  , "xpack.security.enabled: false"
  , "xpack.monitoring.enabled: false"
  , "xpack.watcher.enabled: false"
  , "xpack.ml.enabled: false"
  ]

yieldString :: Monad m => String -> Producer m B.ByteString
yieldString = yield . T.encodeUtf8 . T.pack . (++ "\n")

createElasticDirectory :: FilePath -> IO ()
createElasticDirectory path = do
  createDirectoryIfMissing True path
  callProcess "sudo" ["chown", "elastic:elastic", path]

makeConfig :: NodeConfig -> IO ()
makeConfig nc = do
  writeLog nc "makeConfig"

  createElasticDirectory $ configDirectory nc
  createElasticDirectory $ nodeWorkingDirectory nc </> "data"
  createElasticDirectory $ nodeWorkingDirectory nc </> "logs"

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

  let args = ["run", "--rm"
             , "--name", ncName nodeConfig
             , "--mount", "type=bind,source=" ++ nodeWorkingDirectory nodeConfig </> "data" ++ ",target=/usr/share/elasticsearch/data"
             , "--mount", "type=bind,source=" ++ nodeWorkingDirectory nodeConfig </> "logs" ++ ",target=/usr/share/elasticsearch/logs"
             , "--mount", "type=bind,source=" ++ configDirectory nodeConfig </> "elasticsearch.yml" ++ ",target=/usr/share/elasticsearch/config/elasticsearch.yml"
             , "--network", _unDockerNetwork $ crDockerNetwork $ ncCurrentRun nodeConfig
             , "--ip", ncBindHost nodeConfig
             , "docker.elastic.co/elasticsearch/elasticsearch:5.4.3"
             ]

  writeLog nodeConfig $ "executing: docker " ++ unwords args

  (  ClosedStream
   , (sourceStdout, closeStdout)
   , (sourceStderr, closeStderr)
   , sph) <- streamingProcess $
        (proc "docker" args)
        { cwd = Just $ crWorkingDirectory $ ncCurrentRun nodeConfig
        , env = Just []
        }
{-

Docker used this:

docker network create --driver=bridge --subnet=10.10.10.0/24 --ip-range=10.10.10.0/24 --opt com.docker.network.bridge.name=br0 br0

docker run
  --mount type=bind,source="$(pwd)"/test-data/node1,target=/usr/share/elasticsearch/data
  --mount type=bind,source=/sbin,target=/opt/host/sbin
  --network br0
  --mount type=bind,source="$(pwd)"/docker-elasticsearch.yml,target=/usr/share/elasticsearch/config/elasticsearch.yml
  --name es0
  --rm
  --ip 10.10.10.101
  docker.elastic.co/elasticsearch/elasticsearch:5.4.3

-}

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

signalNode :: ElasticsearchNode -> String -> IO ()
signalNode n@ElasticsearchNode{..} signal = callProcess "docker"
  ["kill", "--signal", signal, ncName esnConfig]

awaitStarted :: ElasticsearchNode -> STM Bool
awaitStarted ElasticsearchNode{..} = saidStarted `orElse` threadExited
  where
  saidStarted = do
    isStarted <- esnIsStarted
    if isStarted then return True else retry

  threadExited = waitSTM esnThread >> return False

awaitExit :: ElasticsearchNode -> STM ExitCode
awaitExit ElasticsearchNode{..} = waitSTM esnThread

callApi :: ToJSON req => ElasticsearchNode -> B.ByteString -> String -> req -> IO (Maybe Value)
callApi node verb path reqBody = do
  rawReq <- parseRequest $ printf "http://%s:%d%s" (ncBindHost $ esnConfig node) (ncHttpPort $ esnConfig node) path
  let req = rawReq
        { method         = verb
        , requestHeaders = requestHeaders rawReq ++ [(hContentType, "application/json")]
        , requestBody    = RequestBodyLBS $ encode reqBody
        }
      manager = crManager $ ncCurrentRun $ esnConfig node

  withResponse req manager $ \response -> decode . BL.fromChunks <$> brConsume (responseBody response)

waitForGreen :: ElasticsearchNode -> IO ()
waitForGreen node = do
  healthResult <- callApi node "GET" "/_cluster/health?wait_for_status=green&timeout=20s" $ object []
  writeLog node $ "waitForGreen: " ++ show healthResult
  when (healthResult ^.. _Just . key "status" . _String /= ["green"]) $ waitForGreen node

main :: IO ()
main = withCurrentRun $ \currentRun -> do

  javaHome <- lookupEnv "JAVA_HOME"

  let nodeConfigs
        = [ NodeConfig
            { ncCurrentRun           = currentRun
            , ncName                 = (if isMaster then "master-" else "data-") ++ show nodeIndex
            , ncIsMasterEligibleNode = isMaster
            , ncIsDataNode           = not isMaster
            , ncHttpPort             = 9200
            , ncPublishPort          = 9300
            , ncJavaHome             = javaHome
            , ncBindHost             = "10.10.10." ++ show (100 + nodeIndex)
            , ncUnicastHosts         = [ ncBindHost nc ++ ":" ++ show (ncPublishPort nc)
                                       | nc <- nodeConfigs
                                       , ncIsMasterEligibleNode nc]
            }
          | nodeIndex <- [1..6]
          , let isMaster = nodeIndex <= 3
          ]

      killRemainingNodes nodes = do
        writeLog currentRun "killing any remaining nodes"
        forM_ nodes $ \n -> do
          isRunning <- atomically $ (awaitExit n >> return False) `orElse` return True
          when isRunning $ do
            signalNode n "KILL"
            void $ atomically $ awaitExit n

      bailOut msg = do
        writeLog currentRun msg
        error msg

  bracket (mapM runNode nodeConfigs) killRemainingNodes $ \nodes -> do

    let faultyNodes = take 2 nodes

    startedFlags <- forM nodes $ \n -> do
      result <- atomically $ awaitStarted n
      writeLog n $ if result then "started successfully" else "did not start successfully"
      return result

    unless (and startedFlags) $ bailOut "not all nodes started successfully"

    createIndexResult <- callApi (head nodes) "PUT" "/synctest" $ object
      [ "settings" .= object
        [ "index" .= object
          [ "number_of_shards"   .= Number 1
          , "number_of_replicas" .= Number 1
          ]
        ]
      , "mappings" .= object
        [ "testdoc" .= object
          [ "properties" .= object
            [ "serial"  .= object [ "type" .= String "integer" ]
            , "updated" .= object [ "type" .= String "long" ]
            ]
          ]
        ]
      ]

    unless (createIndexResult ^.. _Just . key "status" . _Number == [])
      $ bailOut $ "create index failed: " ++ show createIndexResult

    waitForGreen (head nodes)

    state <- callApi (head nodes) "GET" "/_cluster/state" $ object []
    let masterNodes =
          [ nodeName
          | nodeId   <- state ^.. _Just . key "master_node" . _String
          , nodeName <- state ^.. _Just . key "nodes" . key nodeId . key "name" . _String
          ]
    let shardCopies =
          [ (nodeName, isPrimary)
          | routingTableEntry <- state ^.. _Just . key "routing_table" . key "indices" . key "synctest" . key "shards" . key "0" . values
          , nodeId    <- routingTableEntry ^.. key "node" . _String
          , isPrimary <- routingTableEntry ^.. key "primary" . _Bool
          , nodeName  <- state ^.. _Just . key "nodes" . key nodeId . key "name" . _String
          ]

    writeLog currentRun $ "masters: " ++ show masterNodes
    writeLog currentRun $ "shard copies: " ++ show shardCopies

    threadDelay 10000000
    writeLog currentRun "pausing some nodes"
    forM_ faultyNodes $ \n -> signalNode n "STOP"

    threadDelay 100000000
    writeLog currentRun "resuming paused nodes"
    forM_ faultyNodes $ \n -> signalNode n "CONT"

    forever $ threadDelay 10000000

    writeLog currentRun "killing some nodes"
    threadDelay 10000000
    forM_ faultyNodes $ \n -> do
      signalNode n "KILL"
      atomically $ awaitExit n

    writeLog currentRun "terminating all nodes"
    forM_ nodes $ \n -> do
      signalNode n "TERM"
      atomically $ awaitExit n
      writeLog n "terminated"

    writeLog currentRun "finished"
