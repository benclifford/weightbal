{-# LANGUAGE ScopedTypeVariables #-}

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Data.Char (ord)
import Data.IORef
import Data.List
import Data.Maybe
import System.IO
import System.Environment
import System.Random

adj = 0.3

main = do
 l "weightbal"
 args <- getArgs
 l $ "Args: " ++ (show args)

 liveTestList <- readLiveTestList

 l $ "Test list: " ++ (show liveTestList)

 (prevk, prevScores) <- readScores

 l $ "Previous scores: " ++ (show prevScores)

 let newScores = map (\lt -> (lt, fromMaybe (defaultScore prevScores) $ lookup lt prevScores)) liveTestList

 l $ "New scores: " ++ (show newScores)

 p <- partitionShards newScores

 l $ "Partitions: " ++ (show p)

 (nk, nscores) <- runPartitions p prevk

 let newScores' = map (\(n,os) -> (n, fromMaybe os $ lookup n nscores)) newScores

 putStrLn $ "Scores to write out: " ++ (show newScores')
 writeScores (nk, newScores')

l s = hPutStrLn stderr s

defaultScore prev = 1

readLiveTestList :: IO [String]
readLiveTestList =  lines <$> readFile "tests.sim"

readScores :: IO (Double, [(String, Double)])
readScores = read <$> readFile "scores.wb"

writeScores sc = writeFile "scores.wb" (show sc)

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

runPartitions ps pk = do
  l $ "Number of partitions to start: " ++ (show $ length ps)
  mvIDs <- forM ps $ \partition -> do
    mv <- newEmptyMVar
    forkIO $ do
      putStrLn $ "Partition " ++ (show partition)

      let (score :: Double) = fromInteger $ toInteger $ foldr (+) 0 ((ord . head . fst) <$> partition)

      putMVar mv (partition,score :: Double)
    return mv
  kRef <- newIORef pk
  nparts <- forM mvIDs $ \m -> do
    kNow <- readIORef kRef
    putStrLn $ "Waiting for thread..."
    v@(partition, score) <- takeMVar m
    putStrLn $ "Got result: " ++ (show v)
    let prediction = kNow + foldr (+) 0 (snd <$> partition)
    putStrLn $ "Predicted score: " ++ (show prediction)
    putStrLn $ "Actual score: " ++ (show score)
    let e = score - prediction
    putStrLn $ "Error: " ++ (show e)
    let epp = e / prediction
    putStrLn $ "Error per prediction point: " ++ (show epp)
    let app = epp * adj
    putStrLn $ "Adjustment per prediction point: " ++ (show app)
    let npart = map (\(name, score) -> (name, score + score * app)) partition
    writeIORef kRef (kNow + kNow * app)
    putStrLn $ "New partition scores: " ++ (show npart)
    return npart
  let nscores = join nparts
  putStrLn $ "All tested scores: " ++ (show nscores)
  nk <- readIORef kRef
  putStrLn $ "new k: " ++ (show nk)
  return (nk, nscores)

