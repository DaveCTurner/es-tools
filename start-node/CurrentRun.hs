{-# LANGUAGE Rank2Types #-}

module CurrentRun where

import LogContext
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Network.HTTP.Client
import DockerNetwork

data CurrentRun = CurrentRun
  { crName             :: String
  , crWorkingDirectory :: FilePath
  , crWriteLog         :: String -> IO ()
  , crWriteApiLog      :: Integer -> B.ByteString -> String -> BL.ByteString -> BL.ByteString -> IO ()
  , crHttpManager      :: Manager
  , crDockerNetwork    :: DockerNetwork
  , crWithIptablesLock :: forall a. IO a -> IO a
  , crNextId           :: IO Integer
  }

instance LogContext CurrentRun where
  writeLog = crWriteLog
