
module Bal where

  import Data.Function
  import Data.List

  tests = [("a",10), ("b", 20), ("c", 5), ("d", 100)] :: Shard

  type Test = (String, Double)
  type Shard = [Test]
  type Shards = [ Shard ]

  emptyShards = [[], [], []] :: Shards


  xmain = do

    let costedShards = map (\s -> (s, costOfShard s)) emptyShards

    putStrLn $ "Costed shards: " ++ (show costedShards)

    let x0 = foldScoreIntoShards ("a",10) emptyShards
    let x1 = foldScoreIntoShards ("b",20) x0
    putStr "x0: "
    print x0
    putStr "x1: "
    print x1

    let r = foldr foldScoreIntoShards emptyShards tests
    print r 

  costOfShard s = foldr (+) 0 $ map snd s

  foldScoreIntoShards :: Test -> Shards -> Shards
  foldScoreIntoShards t@(name, score) shards = let
    costedShards = map (\s -> (s, costOfShard s)) shards
    sortedShards = sortBy (compare `on` snd) costedShards
    cheapestShard = fst $ head sortedShards
    otherShards = map fst $ tail sortedShards
    in (cheapestShard ++ [t]) : otherShards

