module LogContext where

class LogContext a where
  writeLog :: a -> String -> IO ()

data NoContext = NoContext (String -> IO ())

instance LogContext NoContext where
  writeLog (NoContext f) = f
