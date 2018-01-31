module LogFile where

import Control.Concurrent.MVar
import System.IO

withLogFile :: FilePath -> (Handle -> IO a) -> IO a
withLogFile path go = withFile path AppendMode $ \hLog -> do
  hSetBuffering hLog NoBuffering
  go hLog

data ConcurrentLogFile = ConcurrentLogFile (MVar Handle)

withConcurrentLogFile :: FilePath -> (ConcurrentLogFile -> IO a) -> IO a
withConcurrentLogFile path go = withFile path AppendMode $ \hLog -> do
  lock <- newMVar hLog
  go $ ConcurrentLogFile lock

withLogHandle :: ConcurrentLogFile -> (Handle -> IO a) -> IO a
withLogHandle (ConcurrentLogFile handleVar) go = withMVar handleVar $ \h -> do
  a <- go h
  hFlush h
  return a
