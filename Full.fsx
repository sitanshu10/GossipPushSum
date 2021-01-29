module Full

#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open Akka.FSharp
open Akka.Actor
open Akka.Configuration

let system =
    System.create "system" (Configuration.defaultConfig ())

type command =
    | Activate of int * int * string
    | Activate1 of int * float * float
    | Activate2 of int * int
    | Completion of int

let mutable nodeMap = Map.empty
let maxmessage = 10
let mutable numnodes = 0
let mutable completedActors = Set.empty
let mutable flag = false
let mutable spawnerId = null

let nodePushSum (mailbox: Actor<_>) =
    let mutable s = 0.0
    let mutable w = 0.0
    let mutable sum1 = -1.0
    let mutable sum2 = -1.0
    let mutable sum3 = -1.0

    let rec loop () =
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | Activate1 (actorNum, sum, weight) ->
                if s = 0.0 && w = 0.0 then
                    s <- float actorNum + sum
                    w <- 1.0 + weight
                    sum1 <- s / w
                else
                    s <- s + sum
                    w <- w + weight
                if sum <> 0.0 && weight <> 0.0 then
                    sum3 <- sum2
                    sum2 <- sum1
                    sum1 <- s / w

                    if sum3 <> -1.0 then
                        let mutable diff1 = abs (sum3 - sum2)
                        let mutable diff2 = abs (sum2 - sum1)
                        if diff1 < 0.0000000001 && diff2 < 0.0000000001
                        then spawnerId <! Completion(actorNum)

                let random = Random().Next(1, numnodes + 1)
                if random = actorNum
                   || completedActors.Contains(random) then
                    nodeMap.[actorNum]
                    <! Activate1(actorNum, 0.0, 0.0)
                else
                    s <- s / 2.0
                    w <- w / 2.0
                    nodeMap.[random] <! Activate1(random, s, w)

            | _ -> ()

            return! loop ()
        }

    loop ()

let node (mailbox: Actor<_>) =
    let mutable count = 0

    let rec loop () =
        actor {
            let mutable nodeNum = 0
            let! msg = mailbox.Receive()

            match msg with
            | Activate2 (actorNum, rumor) ->
                count <- count + rumor
                if count = maxmessage then spawnerId <! Completion(actorNum)
                if count < maxmessage then
                    let random = Random().Next(1, numnodes + 1)
                    if random <> actorNum then nodeMap.[random] <! Activate2(random, 1)
                    nodeMap.[actorNum] <! Activate2(actorNum, 0)

            | _ -> ()

            return! loop ()
        }

    loop ()

let spawner (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | Activate (total, startNode, algo) ->
                if algo = "gossip" then
                    printfn
                        "Beginning Gossip Algorithm for Full Topology for %d nodes with start node as %d"
                        numnodes
                        startNode
                    for i = 1 to total do
                        nodeMap <- nodeMap.Add(i, spawn system (sprintf "actor%i" i) node)
                    nodeMap.[startNode] <! Activate2(startNode, 1)
                elif algo = "push-sum" then
                    printfn
                        "Beginning PushSum Algorithm for Full Topology for %d nodes with start node as %d"
                        numnodes
                        startNode
                    for i = 1 to total do
                        nodeMap <- nodeMap.Add(i, spawn system (sprintf "actor%i" i) nodePushSum)
                    nodeMap.[startNode]
                    <! Activate1(startNode, 0.0, 0.0)
                else
                    printfn "Incrorrect Parameters."
                    flag <- true

            | Completion (actor) ->
                completedActors <- completedActors.Add(actor)
                if float completedActors.Count
                   / float numnodes
                   >= 0.9 then
                    flag <- true

            | _ -> ()

            return! loop ()
        }

    loop ()

let start (n: int, a: string) =
    numnodes <- n
    let start = Random().Next(1, n + 1)
    let stopwatch = System.Diagnostics.Stopwatch.StartNew()
    spawnerId <- spawn system "spawner" spawner
    spawnerId <! Activate(numnodes, start, a)
    let mutable y = true
    while y do
        if flag then y <- false
    stopwatch.Stop()
    printfn "Total Time: %f ms" stopwatch.Elapsed.TotalMilliseconds
