{-# LANGUAGE ScopedTypeVariables #-}

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Reader
import Data.Char (ord)
import Data.Either (partitionEithers)
import Data.Function (on)
import Data.IORef
import Data.List
import Data.Maybe
import System.Cmd
import System.Directory (doesFileExist)
import System.Exit
import System.IO
import System.Environment
import System.Random
import System.Time
import Text.Printf

import qualified Bal

data Config = Config {

    -- | This should become a commandline parameter
    -- indicating whether the order of tests within
    -- a partition should be randomized.
    -- This is intended to help find hidden dependencies
    -- within tests by introducing more non-determinism.
    _shuffleOrder :: Bool,

    _adj :: Double,
    _numPartitions :: Int
  }

defaultConfig = Config {
    _shuffleOrder = True,
    _adj = 0.2,
    _numPartitions = 4
  }

type WeightBalEnv = ReaderT Config IO


scoreFilename = "scores.wb"

main :: IO ()
main = do
 l "weightbal"
 args <- getArgs
 l $ "Args: " ++ (show args)
 runReaderT mainW defaultConfig

mainW :: WeightBalEnv ()
mainW = do

 liveTestList <- readLiveTestList

 l $ "Test list: "
 forM_ liveTestList $ \t -> liftIO $ do
   hPutStr stderr "  "
   hPutStrLn stderr t

 scoresExist <- liftIO $ doesFileExist scoreFilename
 (prevk, prevScores) <- if scoresExist then readScores else return (initialDefaultScore,[])

 l $ "Previous scores:"
 dumpScores prevScores

 let newScores = map (\lt -> (lt, fromMaybe (defaultScore prevScores) $ lookup lt prevScores)) liveTestList

 l $ "New scores:"
 dumpScores newScores

 p <- optionallyShufflePartitions =<< partitionShards newScores

 l $ "Partitions: "
 dumpPartitions p

 (res :: Either [Int] (Double, [(String, Double)])) <- runPartitions p prevk
 case res of
   Right (nk, nscores) -> do

     let newScores' = map (\(n,os) -> (n, fromMaybe os $ lookup n nscores)) newScores

     liftIO $ putStrLn $ "Scores to write out:"
     dumpScores newScores'
     writeScores (nk, newScores')
     showNewScores (nk, newScores')

     liftIO $ putStrLn $ "Theoretical test time per shard: " ++ (formatScore $ nk + (foldr1 (+) (map snd newScores')) / (fromInteger $ toInteger $ length p))
     outputXUnit []
     liftIO $ exitSuccess
   Left fails -> do
     liftIO $ putStrLn $ "Outer: some partitions failed: " ++ (show fails)
     outputXUnit fails
     liftIO $ exitFailure

optionallyShufflePartitions :: Bal.Shards -> WeightBalEnv Bal.Shards
optionallyShufflePartitions shards = do
  o <- _shuffleOrder <$> ask
  if not o
    then return shards
    else randomlyPermuteList =<< (mapM randomlyPermuteList shards)
 
randomlyPermuteList :: [e] -> WeightBalEnv [e]
randomlyPermuteList [] = return []
randomlyPermuteList [v] = return [v]
randomlyPermuteList l = do
  pos <- liftIO $ randomRIO (0,length l - 1)
  let v = l !! pos
  let start = take pos l
  let finish = drop (pos+1) l
  let rest = start ++ finish
  permutedRest <- randomlyPermuteList rest
  return $ [v] ++ permutedRest

l s = liftIO $ hPutStrLn stderr s

defaultScore prev = initialDefaultScore -- one minute by default, though this should be calculated as an average of previous tests
initialDefaultScore = 60

readLiveTestList :: WeightBalEnv [String]
readLiveTestList =  liftIO $ lines <$> readFile "tests.sim"

readScores :: WeightBalEnv (Double, [(String, Double)])
readScores = liftIO $ read <$> readFile scoreFilename

writeScores sc = liftIO $ writeFile scoreFilename (show sc)

dumpScores sc = liftIO $ forM_ sc $ \(name, time) -> do
  hPutStr stderr "  "
  hPutStr stderr name
  hPutStr stderr ": "
  hPutStr stderr (formatScore time)
  hPutStr stderr " seconds"
  hPutStrLn stderr ""

dumpPartitions ps = liftIO $ forM_ (ps `zip` [0..]) $ \(p, n) -> do
  hPutStrLn stderr $ "=== Partition " ++ (show n) ++ " ==="
  dumpScores p

partitionShards = partitionShardsBalanced

-- | a pretty bad partitioning function...
partitionShardsRandom scores = do
  l <- forM scores $ \s -> do
    part <- liftIO $ randomRIO (0,2 :: Integer)
    return (s, part)
  return [
      map fst $ filter (\(_,p) -> p == 0) l,
      map fst $ filter (\(_,p) -> p == 1) l,
      map fst $ filter (\(_,p) -> p == 2) l
    ]

-- the number of shards to run is encoded here as unary [] entries
-- in the empty shard list.
partitionShardsBalanced scores = do
  numPartitions <- _numPartitions <$> ask
  let emptyPartitions = take numPartitions $ repeat []
  return $ foldr Bal.foldScoreIntoShards emptyPartitions $ sortBy (compare `on` snd) scores

getTime = (liftIO getClockTime) >>= (\(TOD sec _) -> return sec)

-- take a string with %X single letter substitutions and
-- substitute in the supplied substitutions
subs :: String -> [ (Char, String) ] -> String
('%':k:rest) `subs` l = (fromJust $ lookup k l) ++ (rest `subs` l)
(c:rest) `subs` l = c:(rest `subs` l)
[] `subs` l = []


runPartitions ps pk = do
 adj <- _adj <$> ask
 liftIO $ do
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
      case ec of
        ExitSuccess ->  do
          let (score :: Double) = fromInteger (eTime - sTime)
          putMVar mv (Right (partition,score :: Double))
        ExitFailure f -> do
          putMVar mv (Left f)
    return (np, mv)
  kRef <- newIORef pk
  nparts <- forM mvIDs $ \(np, m) -> do
    kNow <- readIORef kRef
    putStrLn $ "Waiting for partition " ++ (show np)

    thrOut <- takeMVar m
    case thrOut of
      (Right v@(partition, score)) -> do
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
        return $ Right npart
      (Left err) -> return  $ Left np

  let (lefts :: [Int], rights :: [[(String, Double)]]) = partitionEithers nparts
  if lefts == [] then do
    let nscores = join rights
    putStrLn $ "All tested scores:"
    dumpScores nscores
    nk <- readIORef kRef
    putStrLn $ "new k: " ++ (show nk)
    return $ Right (nk, nscores)
   else do
    putStrLn $ "Some partitions failed: " ++ (show lefts)
    return $ Left lefts

showNewScores (nk, nscores) = liftIO $ do
  putStrLn "=== TEST SCORES ==="
  putStrLn $ "Test run startup time: "++(formatScore nk)

  let ss = sortBy (compare `on` snd) nscores

  forM ss $ \(name, score) -> putStrLn $ name ++ ": " ++ (formatScore score)

  putStrLn "=== DONE ==="

formatScore :: Double -> String
formatScore s = printf "%.1f" s

-- | Output an xUnit file
outputXUnit fails = do
 numPartitions <- _numPartitions <$> ask
 liftIO $ writeFile "xunit-weightbal.xml" $
     "<testsuite tests=\"" ++ (show numPartitions) ++ "\">"
  ++ (concat $ map (\n -> shardStatus n) [0..numPartitions-1])
  ++ "</testsuite>"
  where
   shardStatus n = if n `elem` fails then
     "<testcase classname=\"sharding\" name=\"shard" ++ (show n) ++ "\"> <failure type=\"nonzeroReturnCode\">shard failed</failure></testcase>"
    else  "<testcase classname=\"sharding\" name=\"shard" ++ (show n) ++ "\"/>"

