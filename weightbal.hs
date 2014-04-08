{-# LANGUAGE ScopedTypeVariables #-}

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Data.Char (ord)
import Data.List
import Data.Maybe
import System.IO
import System.Environment

numShards = 3

main = do
 l "weightbal"
 args <- getArgs
 l $ "Args: " ++ (show args)

 liveTestList <- readLiveTestList

 l $ "Test list: " ++ (show liveTestList)

 prevScores <- readScores

 l $ "Previous scores: " ++ (show prevScores)

 let newScores = map (\lt -> (lt, fromMaybe (defaultScore prevScores) $ lookup lt prevScores)) liveTestList

 l $ "New scores: " ++ (show newScores)

 let p = partitionShards newScores

 l $ "Partitions: " ++ (show p)

 nscores <- runPartitions p
 writeScores nscores

l s = hPutStrLn stderr s

defaultScore prev = 1

readLiveTestList :: IO [String]
readLiveTestList =  lines <$> readFile "tests.sim"

readScores :: IO [(String, Double)]
readScores = read <$> readFile "scores.wb"

writeScores sc= writeFile "scores.wb" (show sc)

-- | a pretty bad partitioning function...
partitionShards scores = [[head scores], [head $ tail scores], tail $ tail scores] 

runPartitions ps = do
  l $ "Number of partitions to start: " ++ (show $ length ps)
  mvIDs <- forM ps $ \partition -> do
    mv <- newEmptyMVar
    forkIO $ do
      putStrLn $ "Partition " ++ (show partition)

      let (score :: Double) = fromInteger $ toInteger $ foldr1 (+) ((ord . head . fst) <$> partition)

      putMVar mv (partition,score :: Double)
    return mv
  nparts <- forM mvIDs $ \m -> do
    putStrLn $ "Waiting for thread..."
    v@(partition, score) <- takeMVar m
    putStrLn $ "Got result: " ++ (show v)
    let prediction = foldr1 (+) (snd <$> partition)
    putStrLn $ "Predicted score: " ++ (show prediction)
    putStrLn $ "Actual score: " ++ (show score)
    let e = score - prediction
    putStrLn $ "Error: " ++ (show e)
    let epp = e / prediction
    putStrLn $ "Error per prediction point: " ++ (show epp)
    let app = epp * 0.1
    putStrLn $ "Adjustment per prediction point: " ++ (show app)
    let npart = map (\(name, score) -> (name, score + score * app)) partition
    putStrLn $ "New partition scores: " ++ (show npart)
    return npart
  let nscores = join nparts
  putStrLn $ "All scores: " ++ (show nscores)
  return nscores
