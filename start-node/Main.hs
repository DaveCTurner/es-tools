{-# LANGUAGE TupleSections #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import LogFile
import LogContext
import CurrentRun
import DockerNetwork
import Process

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Lens hiding ((.=))
import Control.Monad
import Control.Monad.Except
import Control.Monad.Trans.Resource
import Data.Aeson
import Data.Aeson.Lens
import Data.Conduit
import Data.Conduit.Binary hiding (mapM_, take, head)
import Data.Conduit.Process
import Data.Maybe
import Data.Monoid
import Data.Time
import Data.Time.ISO8601
import Network.HTTP.Client
import Network.HTTP.Types.Header
import System.Directory
import System.Environment
import System.Exit
import System.FilePath
import System.IO
import System.Process.Internals
import System.Random
import System.Timeout
import Text.Printf
import qualified Data.ByteString as B
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

terminateStreamingProcess :: StreamingProcessHandle -> IO ()
terminateStreamingProcess = terminateProcess . streamingProcessHandleRaw

mkEnv :: [(String, Maybe String)] -> Maybe [(String, String)]
mkEnv = Just . mapMaybe (\(k, mv) -> fmap (k,) mv)

logGitVersion :: LogContext a => a -> IO ()
logGitVersion logContext = do
  (_, result, _) <- readProcessWithExitCode "git" ["rev-parse", "HEAD"] ""
  writeLog logContext $ "Git revision " ++ filter (>' ') result

withCurrentRun :: (CurrentRun -> IO a) -> IO a
withCurrentRun go = do
  runName <- formatTime defaultTimeLocale "%Y-%m-%d--%H-%M-%S.%q" <$> getCurrentTime
  cwd <- getCurrentDirectory
  let workingDirectory = cwd </> "output" </> runName
  createElasticDirectory putStrLn workingDirectory

  withConcurrentLogFile (workingDirectory </> "run.log") $ \runLog ->
    withConcurrentLogFile (workingDirectory </> "api.log") $ \apiLog -> do

    let logContext = NoContext $ \msg -> withLogHandle runLog $ \h -> do
          now <- formatISO8601Micros <$> getCurrentTime
          let fullMsg = "[" ++ now ++ "] " ++ msg
          putStrLn fullMsg
          hPutStrLn h fullMsg

        writeApiLog method url request response = withLogHandle apiLog $ \h -> do
          now <- formatISO8601Micros <$> getCurrentTime
          hPutStrLn h $ printf "[%s] %s %s" now (show method) url
          BL.hPut h $ request <> BL.singleton 0x0a <> response <> BL.pack [0x0a, 0x0a]

    manager <- newManager defaultManagerSettings

    writeLog logContext $ "Starting run with working directory: " ++ workingDirectory
    logGitVersion logContext

    withDockerNetwork logContext $ \dockerNetwork ->
      let currentRun = CurrentRun
            { crName             = runName
            , crWorkingDirectory = workingDirectory
            , crWriteLog         = writeLog logContext
            , crWriteApiLog      = writeApiLog
            , crHttpManager      = manager
            , crDockerNetwork    = dockerNetwork
            }
      in withTcpdump currentRun (workingDirectory </> "tcpdump.cap") $ go currentRun

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

instance LogContext NodeConfig where
  writeLog nc = writeLog (ncCurrentRun nc) . printf "[%-9s] %s" (ncName nc)

class HasNodeName a where nodeName :: a -> String

instance HasNodeName NodeConfig where nodeName = ncName

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
  , "discovery.zen.minimum_master_nodes: 1"
  , "discovery.zen.fd.ping_timeout: 2s"
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
  --, "logger.org.elasticsearch.transport: TRACE"
  ]

yieldString :: Monad m => String -> Producer m B.ByteString
yieldString = yield . T.encodeUtf8 . T.pack . (++ "\n")

createElasticDirectory :: (String -> IO ()) -> FilePath -> IO ()
createElasticDirectory writeLogEntry path = do
  createDirectoryIfMissing True path
  let args = ["chown", "elastic:elastic", path]
  writeLogEntry $ "createElasticDirectory: sudo " ++ unwords args
  callProcess "sudo" args

makeConfig :: NodeConfig -> IO ()
makeConfig nc = do
  writeLog nc "makeConfig"

  createElasticDirectory (writeLog nc) $ configDirectory nc
  createElasticDirectory (writeLog nc) $ nodeWorkingDirectory nc </> "data"
  createElasticDirectory (writeLog nc) $ nodeWorkingDirectory nc </> "logs"

  runResourceT $ runConduit
     $  sourceConfig nc
    =$= sinkFile   (configDirectory nc </> "elasticsearch.yml")

writeToConsole :: MonadIO m => ConduitM B.ByteString B.ByteString m ()
writeToConsole = awaitForever $ \bs -> do
  liftIO $ B.putStr bs
  yield bs

checkStarted :: MonadIO m => IO () -> ConduitM B.ByteString B.ByteString m ()
checkStarted onStarted = awaitForever $ \bs ->
  if "started" `B.isInfixOf` bs then do
    liftIO onStarted
    yield bs
    awaitForever yield
  else yield bs

data ElasticsearchNode = ElasticsearchNode
  { esnConfig    :: NodeConfig
  , esnHandle    :: StreamingProcessHandle
  , esnIsStarted :: STM Bool
  , esnThread    :: Async ExitCode
  }

instance LogContext  ElasticsearchNode where writeLog = writeLog . esnConfig
instance HasNodeName ElasticsearchNode where nodeName = nodeName . esnConfig

runNode :: NodeConfig -> IO ElasticsearchNode
runNode nodeConfig = do

  writeLog nodeConfig "runNode"

  makeConfig nodeConfig

  let args = ["run", "--rm"
             , "--name", ncName nodeConfig
             , "--mount", "type=bind,source=" ++ nodeWorkingDirectory nodeConfig </> "data" ++ ",target=/usr/share/elasticsearch/data"
             , "--mount", "type=bind,source=" ++ nodeWorkingDirectory nodeConfig </> "logs" ++ ",target=/usr/share/elasticsearch/logs"
             , "--mount", "type=bind,source=" ++ configDirectory nodeConfig </> "elasticsearch.yml" ++ ",target=/usr/share/elasticsearch/config/elasticsearch.yml"
             , "--network", dockerNetworkId $ crDockerNetwork $ ncCurrentRun nodeConfig
             , "--ip", ncBindHost nodeConfig
             , "--sysctl", "net.ipv4.tcp_retries2=4"
             , "docker.elastic.co/elasticsearch/elasticsearch:6.4.2"
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

  withProcessHandle (streamingProcessHandleRaw sph) $ \case
    OpenHandle pid -> writeLog nodeConfig $ "started with PID " ++ show pid
    _              -> writeLog nodeConfig $ "started but not OpenHandle"

  saidStartedVar <- newTVarIO False

  nodeThread <- async $
    withLogFile (stdoutPath nodeConfig) $ \stdoutLog ->
    withLogFile (stderrPath nodeConfig) $ \stderrLog -> do

    let concurrentConduit = Concurrently . runConduit

        onStarted = do
          writeLog nodeConfig "onStarted"
          atomically $ writeTVar saidStartedVar True

        terminateAndLog = do
          writeLog nodeConfig "terminateAndLog"
          terminateProcess $ streamingProcessHandleRaw sph

    ((), ()) <- runConcurrently ((,)
        <$> concurrentConduit (sourceStdout                    =$= checkStarted onStarted
                                                               =$= sinkHandle stdoutLog)
        <*> concurrentConduit (sourceStderr                    =$= sinkHandle stderrLog))
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
signalNode n signal =
  callProcessNoThrow n "docker" ["kill", "--signal", signal, nodeName n]

awaitStarted :: ElasticsearchNode -> STM Bool
awaitStarted ElasticsearchNode{..} = saidStarted `orElse` threadExited
  where
  saidStarted = do
    isStarted <- esnIsStarted
    if isStarted then return True else retry

  threadExited = waitSTM esnThread >> return False

awaitExit :: ElasticsearchNode -> STM ExitCode
awaitExit ElasticsearchNode{..} = waitSTM esnThread

callApi :: ElasticsearchNode -> B.ByteString -> String -> [Value] -> ExceptT String IO Value
callApi node verb path reqBody = do
  let requestUri = printf "http://%s:%d%s" (ncBindHost $ esnConfig node) (ncHttpPort $ esnConfig node) path
      reqBodyBuilder = case reqBody of
        [] -> mempty
        [oneObject] -> B.lazyByteString (encode oneObject)
        manyObjects -> mconcat [ B.lazyByteString (encode obj) <> B.word8 0x0a | obj <- manyObjects ]
      reqBodyBytes = B.toLazyByteString reqBodyBuilder

  rawReq <- maybe (throwError $ "parseRequest failed: '" ++ requestUri ++ "'") return $ parseRequest requestUri
  let req = rawReq
        { method         = verb
        , requestHeaders = requestHeaders rawReq
                              ++ [(hContentType, "application/json")     | length reqBody == 1]
                              ++ [(hContentType, "application/x-ndjson") | length reqBody >  1]
        , requestBody    = RequestBodyLBS reqBodyBytes
        }
      manager = crHttpManager $ ncCurrentRun $ esnConfig node

      go = withResponse req manager $ \response -> do
        fullResponse <- BL.fromChunks <$> brConsume (responseBody response)
        crWriteApiLog (ncCurrentRun $ esnConfig node) verb requestUri reqBodyBytes fullResponse
        return $ eitherDecode fullResponse
      goSafe = (Right <$> go) `catch` (return . Left)

  liftIO goSafe >>= \case
    Left e -> throwError $ "callApi failed: " ++ show (e :: HttpException)
    Right (Left msg) -> throwError $ "decode failed: " ++ msg
    Right (Right v) -> return v

bothWays :: Applicative m => (ElasticsearchNode -> ElasticsearchNode -> m ()) -> ElasticsearchNode -> ElasticsearchNode -> m ()
bothWays go n1 n2 = (<>) <$> go n1 n2 <*> go n2 n1

runIptables :: [String] -> String -> ElasticsearchNode -> ElasticsearchNode -> IO ()
runIptables args action n1 n2 = 
  callProcess "sudo" $ [ "iptables", action, "DOCKER-USER"
                       , "--source",      ncBindHost (esnConfig n1)
                       , "--destination", ncBindHost (esnConfig n2)
                       , "--protocol", "tcp"
                       , "--wait"
                       ] ++ args

iptablesReject :: String -> ElasticsearchNode -> ElasticsearchNode -> IO ()
iptablesReject = runIptables ["--jump", "REJECT", "--reject-with", "tcp-reset"]

iptablesDrop :: String -> ElasticsearchNode -> ElasticsearchNode -> IO ()
iptablesDrop = runIptables ["--jump", "DROP"]

breakLink :: ElasticsearchNode -> ElasticsearchNode -> IO ()
breakLink = bothWays $ iptablesReject "-I"

unbreakLink :: ElasticsearchNode -> ElasticsearchNode -> IO ()
unbreakLink = bothWays $ iptablesReject "-D"

pauseLink :: ElasticsearchNode -> ElasticsearchNode -> IO ()
pauseLink = bothWays $ iptablesDrop "-I"

unpauseLink :: ElasticsearchNode -> ElasticsearchNode -> IO ()
unpauseLink = bothWays $ iptablesDrop "-D"

main :: IO ()
main = join $ withCurrentRun $ \currentRun -> do

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
          | nodeIndex <- [1..3] :: [Int]
          , let isMaster = nodeIndex <= 1
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

      bailOutOnTimeout t go = maybe (bailOut "bailOutOnTimeout: timed out") return =<< timeout t go

  bracket (mapM runNode nodeConfigs) killRemainingNodes $ \nodes -> do

    let nodesByName = HM.fromList [(nodeName n, n) | n <- nodes]

        retryOnNodes withNode = go (cycle nodes)
          where
          go [] = bailOut "retryOnNodes: impossible: ran out of nodes"
          go (n:ns) = either goAgain return =<< runExceptT (withNode n)
            where
              goAgain msg = do
                writeLog n $ "retryOnNode: failed: " ++ msg
                threadDelay 1000000
                go ns

    startedFlags <- forM nodes $ \n -> do
      result <- atomically $ awaitStarted n
      writeLog n $ if result then "started successfully" else "did not start successfully"
      return result

    unless (and startedFlags) $ bailOut "not all nodes started successfully"

    bailOutOnTimeout 10000000 $ retryOnNodes $ \n -> do
      createIndexResult <- callApi n "PUT" "/i" [object
          [ "settings" .= object
            [ "index" .= object
              [ "number_of_shards"   .= Number 1
              , "number_of_replicas" .= Number 1
              ]
            ]
          ]
        ]

      unless (createIndexResult ^.. key "status" . _Number == []) -- TODO check for "acknowledged" too
        $ throwError $ "create index failed: " ++ show createIndexResult

    let getNodeIdentities = bailOutOnTimeout 600000000 $ retryOnNodes $ \n -> do
          healthResult <- callApi n "GET" "/_cluster/health?wait_for_status=green&timeout=20s" []
          unless (healthResult ^.. key "status" . _String == ["green"])
            $ throwError $ "GET /_cluster/health did not return GREEN"

          state <- callApi n "GET" "/_cluster/state" []

          let nodesFromIds nodeIds =
                [ node
                | nodeId <- nodeIds
                , nodeNameStr <- state ^.. key "nodes" . key nodeId . key "name" . _String . to T.unpack
                , Just node <- [HM.lookup nodeNameStr nodesByName]
                ]

              getUniqueNode nodeType = \case
                [x] -> return x
                notUnique -> throwError $ printf "unique %s not found: got %s" (nodeType::String) (show $ map nodeName notUnique)

          masterNode <- getUniqueNode "master" $ nodesFromIds $ state ^.. key "master_node" . _String

          return masterNode

        withPausedLink n1 n2 go = bracket setup teardown $ const go
          where
          setup = do
            writeLog currentRun $ "pausing link between " ++ nodeName n1 ++ " and " ++ nodeName n2
            pauseLink n1 n2

          teardown () = do
            writeLog currentRun $ "unpausing link between " ++ nodeName n1 ++ " and " ++ nodeName n2
            unpauseLink n1 n2

    master <- getNodeIdentities
    writeLog master  "is master"

    {-
        * index docs to each node (generating fields => mapping updates, including updates, deletes)
        * pause/unpause links between each pair of nodes
    -}

    let indexers = map makeIndexer $ concat $ replicate 2 nodes
          where
          makeIndexer :: ElasticsearchNode -> IO ()
          makeIndexer node = forever $ do
            ops <- replicateM 20 randomOp
            runExceptT $ callApi node "POST" "/i/_doc/_bulk" $ concat ops

        randomOp = do
          indexOrDelete <- randomIO
          docIdNum      <- randomRIO (1000, 9999) :: IO Int
          let bulkAction = object [ "_id" .= T.pack ("doc" ++ show docIdNum) ]

          if indexOrDelete
            then do
              fieldNum <- randomRIO (10, 99) :: IO Int
              let fieldName = T.pack $ "fld" ++ show fieldNum
              return [ object [ "index" .= bulkAction ]
                     , object [ fieldName .= String "blah" ]
                     ]
            else return [ object [ "delete" .= bulkAction ] ]

        networkDisruptors = map makeNetworkDisruptor
          [ (node1, node2)
          | node1 <- nodes
          , node2 <- nodes
          , nodeName (esnConfig node1) < nodeName (esnConfig node2)
          ]
          where
          randomDelay = threadDelay =<< randomRIO (10 * 1000, 1000 * 1000)
          makeNetworkDisruptor (node1, node2) = forever $ do
            randomDelay
            withPausedLink node1 node2 randomDelay

        withAsyncs :: [IO ()] -> IO a -> IO a
        withAsyncs [] go = go
        withAsyncs (action:actions) go = withAsync action $ \_ -> withAsyncs actions go

    withAsyncs (indexers ++ networkDisruptors) $ threadDelay $ 10 * 1000 * 1000
            
    writeLog currentRun "waiting for bulk tasks to finish"

    bailOutOnTimeout (2 * 60 * 1000 * 1000) $ let
      go = do
        void $ runExceptT $ callApi master "GET" "/_cluster/pending_tasks" []
        allTasksOrError <- runExceptT $ callApi master "GET" "/_tasks" []
        case allTasksOrError of
          Left _ -> threadDelay 1000000 >> go
          Right v -> do
            let actions = v ^.. key "nodes" . members . key "tasks" . members . key "action" . _String
                uniqueActions = HS.toList $ HS.fromList actions
            if any (T.isPrefixOf "indices:data/write/bulk") uniqueActions
              then threadDelay 500000 >> go
              else writeLog master $ "no more bulk tasks: " ++ show uniqueActions
      in go

    return (return ())
