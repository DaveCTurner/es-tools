module Process where

import LogContext
import System.Exit
import System.Process
import Text.Printf

callProcessNoThrow :: LogContext a => a -> FilePath -> [String] -> IO ()
callProcessNoThrow logContext exe args = do
  let description = unwords $ exe : args
  writeLog logContext $ "callProcessNoThrow: running " ++ description
  (ec, _, _) <- readProcessWithExitCode exe args ""
  case ec of
    ExitSuccess   -> return ()
    ExitFailure c -> writeLog logContext $ printf "callProcessNoThrow: %s exited with code %d" description c
