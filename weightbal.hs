{-# LANGUAGE ScopedTypeVariables #-}

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Data.Char (ord)
import Data.Function (on)
import Data.IORef
import Data.List
import Data.Maybe
import System.Cmd
import System.Exit
import System.IO
import System.Environment
import System.Random
import System.Time
import Text.Printf

import qualified Bal

adj = 0.2

main = do
 l "weightbal"
 args <- getArgs
 l $ "Args: " ++ (show args)

 liveTestList <- readLiveTestList

 l $ "Test list: "
 forM_ liveTestList $ \t -> do
   hPutStr stderr "  "
   hPutStrLn stderr t

 (prevk, prevScores) <- readScores

 l $ "Previous scores:"
 dumpScores prevScores

 let newScores = map (\lt -> (lt, fromMaybe (defaultScore prevScores) $ lookup lt prevScores)) liveTestList

 l $ "New scores:"
 dumpScores newScores

 p <- partitionShards newScores

 l $ "Partitions: "
 dumpPartitions p

 (nk, nscores) <- runPartitions p prevk

 let newScores' = map (\(n,os) -> (n, fromMaybe os $ lookup n nscores)) newScores

 putStrLn $ "Scores to write out:"
 dumpScores newScores'
 writeScores (nk, newScores')
 showNewScores (nk, newScores')

 putStrLn $ "Theoretical test time per shard: " ++ (formatScore $ nk + (foldr1 (+) (map snd newScores')) / (fromInteger $ toInteger $ length p))

l s = hPutStrLn stderr s

defaultScore prev = 60 -- one minute by default, though this should be calculated as an average of previous tests

readLiveTestList :: IO [String]
readLiveTestList =  lines <$> readFile "tests.sim"

readScores :: IO (Double, [(String, Double)])
readScores = read <$> readFile "scores.wb"

writeScores sc = writeFile "scores.wb" (show sc)

dumpScores sc = forM_ sc $ \(name, time) -> do
  hPutStr stderr "  "
  hPutStr stderr name
  hPutStr stderr ": "
  hPutStr stderr (formatScore time)
  hPutStr stderr " seconds"
  hPutStrLn stderr ""

dumpPartitions ps = forM_ (ps `zip` [0..]) $ \(p, n) -> do
  hPutStrLn stderr $ "=== Partition " ++ (show n) ++ " ==="
  dumpScores p

partitionShards = partitionShardsBalanced

-- | a pretty bad partitioning function...
partitionShardsRandom scores = do
  l <- forM scores $ \s -> do
    part <- randomRIO (0,2 :: Integer)
    return (s, part)
  return [
      map fst $ filter (\(_,p) -> p == 0) l,
      map fst $ filter (\(_,p) -> p == 1) l,
      map fst $ filter (\(_,p) -> p == 2) l
    ]

-- the number of shards to run is encoded here as unary [] entries
-- in the empty shard list.
partitionShardsBalanced scores = return $ foldr Bal.foldScoreIntoShards [ [], [], [], [] ] $ sortBy (compare `on` snd) scores

getTime = getClockTime >>= (\(TOD sec _) -> return sec)

-- take a string with %X single letter substitutions and
-- substitute in the supplied substitutions
subs :: String -> [ (Char, String) ] -> String
('%':k:rest) `subs` l = (fromJust $ lookup k l) ++ (rest `subs` l)
(c:rest) `subs` l = c:(rest `subs` l)
[] `subs` l = []


runPartitions ps pk = do
  templateCLI <- (join . (intersperse " ")) <$> getArgs
  l $ "Number of partitions to start: " ++ (show $ length ps)
  let numberedPartition = [0..] `zip` ps
  mvIDs <- forM numberedPartition $ \(np, partition) -> do
    mv <- newEmptyMVar
    forkIO $ do
      putStrLn $ "Partition:" 
      dumpScores partition
      let testNames = join $ intersperse " " (fst <$> partition)
      let shardnum = np
      let cmd = templateCLI `subs` [ ('S', (show shardnum))
                                   , ('T', testNames)
                                   ]
--      let cmd = "ssh -i ~/.ssh/id_root root@lulu.xeus.co.uk /home/benc/dockerfiles/functional-test-client2 " ++ commitid ++ " " ++ (show shardnum) ++ " http://${S3HOST}:1606" ++ (show shardnum) ++ "/xeus/ " ++ (testNames)
      -- let cmd = "sleep " ++ (show $ ( fromInteger $ toInteger $ foldr (+) 0 ((ord . head . fst) <$> partition)) `div` 20 )
      putStrLn $ "Will run: " ++ cmd
      sTime <- getTime
      ec <- system $ cmd
      eTime <- getTime
      when (ec /= ExitSuccess) $ exitFailure

      let (score :: Double) = fromInteger (eTime - sTime)

      putMVar mv (partition,score :: Double)
    return mv
  kRef <- newIORef pk
  nparts <- forM mvIDs $ \m -> do
    kNow <- readIORef kRef
    putStrLn $ "Waiting for thread..."
    v@(partition, score) <- takeMVar m
    putStrLn $ "Got result: " ++ (show v)
    let prediction = kNow + foldr (+) 0 (snd <$> partition)
    putStrLn $ "Predicted time: " ++ (formatScore prediction) ++ "s"
    putStrLn $ "Actual time: " ++ (formatScore score) ++ "s"
    let e = score - prediction
    putStrLn $ "Error: " ++ (formatScore e) ++ "s"
    let epp = e / prediction
    putStrLn $ "Error per prediction point: " ++ (formatScore epp) ++ "s"
    let app = epp * adj
    putStrLn $ "Adjustment per prediction point: " ++ (show app) ++ "s"
    let npart = map (\(name, score) -> (name, score + score * app)) partition

    let numParts = fromInteger $ toInteger $ length ps
    let kApp = (1+app) ** (1/numParts)
    -- use a different scale for k to attent to account for the fact
    -- that it happens once per partition, not once per run
    writeIORef kRef (kNow * kApp)

    putStrLn $ "New partition scores:"
    dumpScores npart
    return npart
  let nscores = join nparts
  putStrLn $ "All tested scores:"
  dumpScores nscores
  nk <- readIORef kRef
  putStrLn $ "new k: " ++ (show nk)
  return (nk, nscores)

showNewScores (nk, nscores) = do
  putStrLn "=== TEST SCORES ==="
  putStrLn $ "Test run startup time: "++(formatScore nk)

  let ss = sortBy (compare `on` snd) nscores

  forM ss $ \(name, score) -> putStrLn $ name ++ ": " ++ (formatScore score)

  putStrLn "=== DONE ==="

formatScore :: Double -> String
formatScore s = printf "%.1f" s

