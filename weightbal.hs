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
import Data.Traversable (for)
import System.Cmd
import System.Console.GetOpt
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
    _requestedPartitions :: Int,
    _args :: [String],
    _scoreFilename :: String,
    _testsFilename :: String,
    _variablePartitions :: Bool,
    _fakeTest :: Bool
  }

defaultConfig = Config {
    _shuffleOrder = True,
    _adj = 0.3,
    _requestedPartitions = 4,
    _args = [],
    _scoreFilename = "scores.wb",
    _testsFilename = "tests.sim",
    _variablePartitions = False,
    _fakeTest = False
  }

shardRuntimeDecayFactor = 0.8

-- | The fraction of the time that we will jiggle the number of partitions
jiggleFraction = 0.4

type WeightBalEnv = ReaderT Config IO

cliOptions = [
    Option "n" ["partitions"] (ReqArg (\param -> \c -> c { _requestedPartitions = read param} ) "NUM") "Number of partitions"
  , Option "s" ["scores"] (ReqArg (\param -> \c -> c { _scoreFilename = param} ) "FILENAME") "Filename to buffer scores in between runs"
  , Option "l" ["list"] (ReqArg (\param -> \c -> c { _testsFilename = param} ) "FILENAME") "Filename of test list"
  , Option "v" ["variable"] (NoArg (\c -> c { _variablePartitions = True } )) "Vary number of partitions to find a good number"
  , Option "" ["fake"] (NoArg (\c -> c { _fakeTest = True } )) "Fake test mode"
  ]


main :: IO ()
main = do
 l "weightbal"
 cli <- getArgs

 let (os, args, unrecogniseds, errors) = getOpt' RequireOrder cliOptions cli

 when (unrecogniseds /= []) $ error $ "Unrecognised command line parameter(s): " ++ (join $ intersperse ", " unrecogniseds)
 when (errors /= []) $ error $ "Command line parameter error(s): " ++ (join $ intersperse ", " errors)

 let config = defaultConfig { _args = args }

 let config' = applyAll os config

 runReaderT mainW config'

applyAll :: [a->a] -> a -> a
applyAll [] v = v
applyAll (f:fs) v = f (applyAll fs v)

mainW :: WeightBalEnv ()
mainW = do

 liveTestList <- readLiveTestList

 l $ "List of specified tests: "
 forM_ liveTestList $ \t -> liftIO $ do
   hPutStr stderr "  "
   hPutStrLn stderr t

 scoreFilename <- _scoreFilename <$> ask
 scoresExist <- liftIO $ doesFileExist scoreFilename
 (prevk, prevScores) <- if scoresExist then readScores else return (initialDefaultScore,[])


 l $ "Scores loaded from previous run:"
 dumpScores prevScores

 let (newScores, unusedScores) = partitionScores liveTestList prevScores

 l $ "Scores loaded from previous run that are still relevant:"
 dumpScores newScores

 l $ "Scores from previous run that are not relevant in this run:"
 dumpScores unusedScores

 shardScoreFilename <- generateShardScoreFilename
 shardScoresExist <- liftIO $ doesFileExist shardScoreFilename
 shardScoreList <- if shardScoresExist then readShardScores else return []

 l $ "Shard scores loaded from previous run:"
 liftIO $ mapM_ printShardScore $ sortBy (compare `on` fst) shardScoreList

 numPartitions <- _requestedPartitions <$> ask
 variablePartitions <- _variablePartitions <$> ask

 let scoresToRun = newScores
 numPartitionsToRun <- if variablePartitions
   then jiggle numPartitions (cheapestPartition numPartitions shardScoreList)
   else return numPartitions

--  let prevnpk = prevNPKFor prevk shardScoreList numPartitions

 p <- optionallyShufflePartitions =<< partitionShards numPartitionsToRun scoresToRun

 l $ "Partitions:"
 dumpPartitions p

 (res :: Either [Int] (Double, [(String, Double)], Double)) <- runPartitions p prevk
 case res of
   Right (nk, nscores, maxScore) -> do

     let newScores' = map (\(n,os) -> (n, fromMaybe os $ lookup n nscores)) scoresToRun

     let scoresToStore = newScores' ++ unusedScores
     liftIO $ putStrLn $ "Scores to write out:"
     dumpScores scoresToStore
     writeScores (nk, scoresToStore)
     showNewScores (nk, scoresToStore)

     let updatedShardScoreList = updateShardScores numPartitionsToRun shardScoreList maxScore

     writeShardScores updatedShardScoreList

     liftIO $ putStrLn $ "Theoretical test time per shard: " ++ (formatScore $ nk + (foldr1 (+) (map snd newScores')) / (fromInteger $ toInteger $ length p))
     outputXUnit []
     liftIO $ exitSuccess
   Left fails -> do
     liftIO $ putStrLn $ "Some partitions failed: "
     for fails $ \p -> liftIO $ putStrLn $ "  Partition " ++ (show p) ++ " FAILED"
     outputXUnit fails
     liftIO $ exitFailure

updateShardScores np scores score = let
  l = lookup np scores
  scoresWithout = filter (\(n,_) -> n /= np) scores
  in case l of
    Nothing -> (np, score) : scores
    Just oldScore -> (np, oldScore * shardRuntimeDecayFactor + score * ( 1 - shardRuntimeDecayFactor ) ) : scoresWithout
cheapestPartition maxShards [] = maxShards
cheapestPartition _ shardScores = fst $ head $ sortBy (compare `on` snd) shardScores

jiggle :: Int -> Int -> ReaderT Config IO Int
jiggle maxPartitions chosenPartitions = do
  jiggleNum <- liftIO $ randomRIO (0, 1 :: Double)
  if jiggleNum > jiggleFraction
    then return chosenPartitions
    else do upDown <- liftIO $ randomRIO (False, True)
            case upDown of
              False -> return $ (chosenPartitions + 1) `min` maxPartitions
              True -> return $ (chosenPartitions - 1) `max` 1
  
{-
prevNPKFor prevk (shardScoreList :: [(Int, Double)]) numPartitions = let
  real = lookup numPartitions shardScoreList
  mean = (foldr1 (+) $ map snd shardScoreList) / (fromInteger $ toInteger $ length shardScoreList)
  in case real of
    Just v -> v -- if we have a score, use that
    Nothing | shardScoreList /= [] -> mean -- if we have no score, use the mean
    Nothing | shardScoreList == [] -> defaultShardCost -- and if we have no shard scores at all...
-}

partitionScores liveTestList prevScores = let
  newScores = map (\lt -> (lt, fromMaybe (defaultScore prevScores) $ lookup lt prevScores)) liveTestList

  unusedScores = filter (\(k,v) -> not (k `elem` liveTestList)) prevScores
  in (newScores, unusedScores)

{-
generateVariablePartitionAndScores numPartitions scores shardScores prevk = do

  -- for every number of nodes from 1 .. numPartitions:
  pCosts <- forM [1..numPartitions] $ \np -> do
    liftIO $ putStrLn $ "Considering case with " ++ (show np) ++ " workers"
    -- score is the score for the base and the set of tests, but with
    -- another constant added in that is the constant for that number of
    -- nodes.
    -- Partition the scores for this number of partitions, and then
    -- add on the appropriate constant for this number of partitions.
    -- We do not need to consider the global startup constant.
    ps <- partitionShards np scores
    liftIO $ putStrLn $ "ps = " ++ (show ps)
    liftIO $ putStrLn $ "length ps = " ++ (show $ length ps)
    let npCost = prevNPKFor prevk shardScores np
    liftIO $ putStrLn $ "npCost = " ++ (show npCost)
    let testsCost = foldr1 max -- find the biggest cost
                  $ map (foldr (+) 0)  -- add the costs in each partition
                  $ map (map snd) ps -- drop the test name and leave the cost
    let partitionCost = testsCost + npCost
    -- XXX TODO: add in the per-np overhead here
    liftIO $ putStrLn $ "This partitioning costs " ++ (show partitionCost) ++ "s"
    return (np, partitionCost)

  liftIO $ do
    putStrLn "Costs by number of partitions: "
    forM_ pCosts $ \(np, cost) -> do
      putStrLn $ (show np) ++ " partitions -> " ++ (show cost) ++ "s"

  let cheapestPartition = fst $ head $ sortBy (compare `on` snd) pCosts

  return cheapestPartition
-}

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

defaultScore prev = if prev == [] then initialDefaultScore
  else median $ sort (snd <$> prev)

initialDefaultScore = 60

readLiveTestList :: WeightBalEnv [String]
readLiveTestList =  do
  testsFilename <- _testsFilename <$> ask
  liftIO $ lines <$> readFile testsFilename

readScores :: WeightBalEnv (Double, [(String, Double)])
readScores = do
  scoreFilename <- _scoreFilename <$> ask
  liftIO $ read <$> readFile scoreFilename

writeScores sc = do
  scoreFilename <- _scoreFilename <$> ask
  liftIO $ writeFile scoreFilename (show sc)

readShardScores :: WeightBalEnv [(Int, Double)]
readShardScores = do
  scoreFilename <- generateShardScoreFilename
  liftIO $ read <$> readFile scoreFilename

printShardScore (num, time) = putStrLn $ (show num) ++ " shards: " ++ (formatScore time) ++ "s"

writeShardScores shardScores = do
  filename <- generateShardScoreFilename
  liftIO $ writeFile filename (show shardScores)

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

-- the number of shards to run is encoded here as unary [] entries
-- in the empty shard list.
partitionShards numPartitions scores = do
  let emptyPartitions = take numPartitions $ repeat []
  return $ foldr Bal.foldScoreIntoShards emptyPartitions $ sortBy (compare `on` snd) scores

getTime = (liftIO getClockTime) >>= (\(TOD sec _) -> return sec)

-- take a string with %X single letter substitutions and
-- substitute in the supplied substitutions
subs :: String -> [ (Char, String) ] -> String
('%':k:rest) `subs` l = (fromMaybe (error ("Unknown substitution %"++[k])) 
                                   (lookup k l)
                        )
                        ++ (rest `subs` l)
(c:rest) `subs` l = c:(rest `subs` l)
[] `subs` l = []

generateShardScoreFilename = do
  base <- _scoreFilename <$> ask
  return $ base ++ ".ssf2"

runPartitions ps pk = do
 fakeTest <- _fakeTest <$> ask
 adj <- _adj <$> ask
 args <- _args <$> ask
 liftIO $ do
  let templateCLI = (join . (intersperse " ")) args
  l $ "Number of partitions to start: " ++ (show $ length ps)
  let numberedPartition = [0..] `zip` ps
  mvIDs <- forM numberedPartition $ \(np, partition) -> do
    mv <- newEmptyMVar
    putStrLn $ "Partition:" 
    dumpScores partition
    let testNames = join $ intersperse " " (fst <$> partition)
    let shardnum = np
    let cmd = templateCLI `subs` [ ('S', (show shardnum)) -- number of this shard
                                 , ('T', testNames) -- list of tests for this partition
                                 , ('N', (show (length ps))) -- number of shards
                                 , ('%', "%") -- constant %
                                 ]
    putStrLn $ "Will run: " ++ cmd
    forkIO $
      if not fakeTest
      then do
        sTime <- getTime
        ec <- system $ cmd
        eTime <- getTime
        case ec of
          ExitSuccess ->  do
            let (score :: Double) = fromInteger (eTime - sTime)
            putMVar mv (Right (partition,score :: Double))
          ExitFailure f -> do
            putMVar mv (Left f)
      else putMVar mv (Right (partition, fromInteger $ toInteger $ (length ps) * 7 + (foldr1 (+) $ map (read . fst) partition)))
    return (np, mv)
  kRef <- newIORef pk
  maxScoreRef <- newIORef 0
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
    -- that it happens once per partition, not once per run.
        writeIORef kRef (kNow * kApp)

        putStrLn $ "New partition scores:"
        dumpScores npart

        atomicModifyIORef maxScoreRef (\s -> if score > s then (score, ()) else (s,()))

        return $ Right npart
      (Left err) -> return  $ Left np

  let (lefts :: [Int], rights) = partitionEithers nparts
  if lefts == [] then do
    maxScore <- readIORef maxScoreRef
    putStrLn $ "maxScore = " ++ (show maxScore)
    let nscores = join rights
    putStrLn $ "All tested scores:"
    dumpScores nscores
    nk <- readIORef kRef
    putStrLn $ "new k: " ++ (show nk)
    return $ Right (nk, nscores, maxScore)
   else return $ Left lefts

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
 numPartitions <- _requestedPartitions <$> ask
 liftIO $ writeFile "xunit-weightbal.xml" $
     "<testsuite tests=\"" ++ (show numPartitions) ++ "\">"
  ++ (concat $ map (\n -> shardStatus n) [0..numPartitions-1])
  ++ "</testsuite>"
  where
   shardStatus n = if n `elem` fails then
     "<testcase classname=\"sharding\" name=\"shard" ++ (show n) ++ "\"> <failure type=\"nonzeroReturnCode\">shard failed</failure></testcase>"
    else  "<testcase classname=\"sharding\" name=\"shard" ++ (show n) ++ "\"/>"

median l = l !! (length l `div` 2)

