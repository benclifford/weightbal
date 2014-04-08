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
import System.Random

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

 p <- partitionShards newScores

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
partitionShards scores = do
  l <- forM scores $ \s -> do
    part <- randomRIO (0,2 :: Integer)
    return (s, part)
  return [
      map fst $ filter (\(_,p) -> p == 0) l,
      map fst $ filter (\(_,p) -> p == 1) l,
      map fst $ filter (\(_,p) -> p == 2) l
    ]

runPartitions ps = do
  l $ "Number of partitions to start: " ++ (show $ length ps)
  mvIDs <- forM ps $ \partition -> do
    mv <- newEmptyMVar
    forkIO $ do
      putStrLn $ "Partition " ++ (show partition)

      let (score :: Double) = fromInteger $ toInteger $ foldr (+) 0 ((ord . head . fst) <$> partition)

      putMVar mv (partition,score :: Double)
    return mv
  nparts <- forM mvIDs $ \m -> do
    putStrLn $ "Waiting for thread..."
    v@(partition, score) <- takeMVar m
    putStrLn $ "Got result: " ++ (show v)
    let prediction = foldr (+) 0 (snd <$> partition)
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
