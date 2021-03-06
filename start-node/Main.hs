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
import qualified Data.Sequence as Seq
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

    withDockerNetwork logContext $ \dockerNetwork -> go CurrentRun
      { crName             = runName
      , crWorkingDirectory = workingDirectory
      , crWriteLog         = writeLog logContext
      , crWriteApiLog      = writeApiLog
      , crHttpManager      = manager
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
  , "discovery.zen.minimum_master_nodes: 2"
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
  , "logger.org.elasticsearch.action.bulk: TRACE"
  , "logger.org.elasticsearch.cluster.service: DEBUG"
  , "logger.org.elasticsearch.indices.recovery: TRACE"
  , "logger.org.elasticsearch.index.shard: TRACE"
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
             , "docker.elastic.co/elasticsearch/elasticsearch:6.1.2"
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
          | nodeIndex <- [1..5] :: [Int]
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
      createIndexResult <- callApi n "PUT" "/synctest" [object
          [ "settings" .= object
            [ "index" .= object
              [ "number_of_shards"   .= Number 1
              , "number_of_replicas" .= Number 1
              ]
            ]
          , "mappings" .= object
            [ "testdoc" .= object
              [ "properties" .= object []
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

          let shardCopyNodeIds =
                [ (nodeId, isPrimary)
                | routingTableEntry <- state ^.. key "routing_table" . key "indices" . key "synctest" . key "shards" . key "0" . values
                , nodeId    <- routingTableEntry ^.. key "node" . _String
                , isPrimary <- routingTableEntry ^.. key "primary" . _Bool
                ]

          primaryNode <- getUniqueNode "primary" $ nodesFromIds [nid | (nid, True)  <- shardCopyNodeIds]
          replicaNode <- getUniqueNode "replica" $ nodesFromIds [nid | (nid, False) <- shardCopyNodeIds]

          return (masterNode, primaryNode, replicaNode)

    do
      (master, primary, replica) <- getNodeIdentities
      writeLog master  "is master"
      writeLog primary "is primary"
      writeLog replica "is replica"

      let masterBackups@[masterBackup1, masterBackup2] = [ n | n <- nodes, ncIsMasterEligibleNode (esnConfig n), ncName (esnConfig n) /= ncName (esnConfig master) ]
      writeLog masterBackup1 "is master backup"
      writeLog masterBackup2 "is master backup"

      writeLog currentRun $ "breaking link between " ++ nodeName primary ++ " and " ++ nodeName replica
      breakLink primary replica

      writeLog currentRun $ "pausing link between " ++ nodeName master ++ " and " ++ nodeName replica
      pauseLink master replica

      forM_ masterBackups $ \mb -> do
        writeLog currentRun $ "pausing link between " ++ nodeName master ++ " and " ++ nodeName mb
        pauseLink master mb
        writeLog currentRun $ "pausing link between " ++ nodeName primary ++ " and " ++ nodeName mb
        pauseLink primary mb

      let indexDoc = do
            threadDelay 1000000
            writeLog primary "indexing document"
            void $ runExceptT $ callApi primary "PUT" "/synctest/testdoc/testid" [object[]]
            writeLog primary "indexing document finished"

      withAsync indexDoc $ \indexDocAsync -> do
        threadDelay 500000
        iptablesDrop "-D" master masterBackup1
        threadDelay 1000000
        iptablesDrop "-I" master masterBackup1
        threadDelay 500000
        iptablesDrop "-D" masterBackup1 master
        threadDelay 1000000
        iptablesDrop "-I" masterBackup1 master

        wait indexDocAsync

      threadDelay 100000000

      writeLog currentRun $ "unbreaking link between " ++ nodeName primary ++ " and " ++ nodeName replica
      unbreakLink primary replica

      writeLog currentRun $ "unpausing link between " ++ nodeName master ++ " and " ++ nodeName replica
      unpauseLink master replica

      forM_ masterBackups $ \mb -> do
        writeLog currentRun $ "unpausing link between " ++ nodeName master ++ " and " ++ nodeName mb
        unpauseLink master mb
        writeLog currentRun $ "unpausing link between " ++ nodeName primary ++ " and " ++ nodeName mb
        unpauseLink primary mb

      return (return ())

data GeneratorState = GeneratorState
  { gsNextSerial  :: Int
  , gsCurrentDocs :: Seq.Seq Int
  , gsRng         :: StdGen
  }

data Action = CreateDoc !Int | UpdateDoc !Int !Int | DeleteDoc !Int deriving (Show, Eq)

encodeAction :: Action -> [Value]
encodeAction (CreateDoc docId) =
  [ object ["index" .= object ["_id" .= docId]]
  , object ["serial" .= docId, "updated" .= docId]
  ]
encodeAction (UpdateDoc docId actId) =
  [ object ["update" .= object ["_id" .= docId]]
  , object ["doc" .= object ["serial" .= docId, "updated" .= actId]]
  ]
encodeAction (DeleteDoc docId) =
  [ object ["delete" .= object ["_id" .= docId]]
  ]

withTrafficGenerator :: [ElasticsearchNode] -> IO a -> IO a
withTrafficGenerator allNodes go = do
  rng <- getStdGen
  stateVar <- newTVarIO $ GeneratorState 0 Seq.empty rng
  let logStart = forM_ allNodes $ \n -> writeLog n "withTrafficGenerator: starting"
      logEnd   = forM_ allNodes $ \n -> writeLog n "withTrafficGenerator: finished"
      spawnGenerators [] = go `finally` logEnd
      spawnGenerators (n:ns) = withAsync (generateTrafficTo n stateVar) $ \_ -> spawnGenerators ns
  logStart
  spawnGenerators $ concat $ replicate 20 allNodes

generateTrafficTo :: ElasticsearchNode -> TVar GeneratorState -> IO ()
generateTrafficTo node stateVar = forever $ do
    actions <- replicateM 1000 $ atomically $ do
      actionId <- nextSerial
      docs <- gsCurrentDocs <$> readTVar stateVar
      let currentDocCount = Seq.length docs
      let maxPosition = max currentDocCount 10000
      position <- withRng $ randomR (0, maxPosition)
      if position < currentDocCount
        then do
          let docId = Seq.index docs position
          shouldDelete <- (< 0.2) <$> withRng (randomR (0.0, 1.0 :: Double))
          if shouldDelete
            then deleteDoc position docId
            else return $! UpdateDoc docId actionId

        else createDoc actionId

    void $ runExceptT $ callApi node "POST" "/synctest/testdoc/_bulk" $ concatMap encodeAction actions

  where

  nextSerial :: STM Int
  nextSerial = do
    gs <- readTVar stateVar
    let n = gsNextSerial gs
    writeTVar stateVar $ gs { gsNextSerial = n + 1}
    return n

  withRng f = do
    gs <- readTVar stateVar
    let (a, rng') = f (gsRng gs)
    writeTVar stateVar gs { gsRng = rng' }
    return a

  updateCurrentDocs f = modifyTVar stateVar $ \gs -> gs { gsCurrentDocs = f $ gsCurrentDocs gs }

  createDoc docId = do
    updateCurrentDocs (Seq.|> docId)
    return $! CreateDoc docId

  deleteDoc position docId = do
    updateCurrentDocs (deleteSeqElemAt position)
    return $! DeleteDoc docId

deleteSeqElemAt :: Int -> Seq.Seq a -> Seq.Seq a
deleteSeqElemAt i xs = before <> Seq.drop 1 notBefore
  where
  (before, notBefore) = Seq.splitAt i xs
