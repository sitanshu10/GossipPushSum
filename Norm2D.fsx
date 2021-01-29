module Norm2D

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

let mutable neighMap = Map.empty
let mutable nodeMap = Map.empty
let maxmessage = 10
let mutable numnodes = 0
let mutable root = 0
let mutable completedActors = Set.empty
let mutable flag = false
let mutable spawnerId = null
let mutable flag1 = true

let nodePushSum (mailbox: Actor<_>) =
    let mutable count = 0
    let mutable s = 0.0
    let mutable w = 0.0
    let mutable sum1 = -1.0
    let mutable sum2 = -1.0
    let mutable sum3 = -1.0
    let mutable x = 0.0
    let mutable y = 0.0
    let mutable flagabcd = false

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
                        x <- diff1
                        y <- diff2
                        if diff1 < 0.0000000001 && diff2 < 0.0000000001
                        then spawnerId <! Completion(actorNum)

                let mutable list1 = []
                list1 <- neighMap.[actorNum]

                let mutable count = 0
                let mutable index = 0
                let mutable flagabcd = true
                for i = 0 to list1.Length - 1 do
                    if not (completedActors.Contains(list1.[i])) then
                        flagabcd <- false
                        count <- count + 1
                        index <- i
                if flagabcd then
                    let mutable random = (System.Random()).Next(1, numnodes + 1)
                    while random = actorNum
                          || completedActors.Contains(random) do
                        random <- (System.Random()).Next(1, numnodes + 1)
                    s <- s / 2.0
                    w <- w / 2.0
                    nodeMap.[random] <! Activate1(random, s, w)
                else
                    s <- s / 2.0
                    w <- w / 2.0
                    if count = 1 then
                        nodeMap.[list1.[index]]
                        <! Activate1(list1.[index], s, w)
                    else
                        let mutable random = (System.Random()).Next(0, list1.Length)
                        while completedActors.Contains(list1.[random]) do
                            random <- (System.Random()).Next(0, list1.Length)
                        nodeMap.[list1.[random]]
                        <! Activate1(list1.[random], s, w)

            | _ -> ()

            return! loop ()
        }

    loop ()

let node (mailbox: Actor<_>) =
    let mutable count = 0

    let rec loop () =
        actor {
            let mutable list1 = []
            let mutable random = 0
            let! msg = mailbox.Receive()

            match msg with
            | Activate2 (actorNum, rumor) ->
                count <- count + rumor
                if count = maxmessage then spawnerId <! Completion(actorNum)
                if count < maxmessage then
                    list1 <- neighMap.[actorNum]
                    random <- (System.Random()).Next(0, list1.Length)
                    nodeMap.[list1.[random]]
                    <! Activate2(list1.[random], 1)
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
                        "Beginning Gossip Algorithm for 2D Topology for %d nodes with start node as %d"
                        numnodes
                        startNode
                    neighMap <- neighMap.Add(1, [ 2; 1 + root ])
                    neighMap <- neighMap.Add(numnodes, [ numnodes - 1; numnodes - root ])
                    nodeMap <- nodeMap.Add(1, spawn system (sprintf "actor1") node)
                    nodeMap <- nodeMap.Add(numnodes, spawn system (sprintf "actor%i" numnodes) node)
                    for i = 2 to numnodes - 1 do
                        if i = root then // Top right corner
                            neighMap <- neighMap.Add(i, [ i - 1; i + root ])
                        elif i + root - 1 = numnodes then // Bottom Left Corner
                            neighMap <- neighMap.Add(i, [ i - root; i + 1 ])
                        elif i < root then //First row
                            neighMap <- neighMap.Add(i, [ i - 1; i + 1; i + root ])
                        elif i + root - 1 > numnodes then //Last row
                            neighMap <- neighMap.Add(i, [ i - 1; i + 1; i - root ])
                        elif i % root = 0 then //Last column
                            neighMap <- neighMap.Add(i, [ i - root; i + root; i - 1 ])
                        elif (i - 1) % root = 0 then //First column
                            neighMap <- neighMap.Add(i, [ i - root; i + root; i + 1 ])
                        else //Center nodes
                            neighMap <- neighMap.Add(i, [ i + root; i - root; i - 1; i + 1 ])
                        nodeMap <- nodeMap.Add(i, spawn system (sprintf "actor%i" i) node)
                    nodeMap.[startNode] <! Activate2(startNode, 1)
                elif algo = "push-sum" then
                    printfn
                        "Beginning PushSum Algorithm for 2D Topology for %d nodes with start node as %d"
                        numnodes
                        startNode
                    neighMap <- neighMap.Add(1, [ 2; 1 + root ])
                    neighMap <- neighMap.Add(numnodes, [ numnodes - 1; numnodes - root ])
                    nodeMap <- nodeMap.Add(1, spawn system (sprintf "actor1") nodePushSum)
                    nodeMap <- nodeMap.Add(numnodes, spawn system (sprintf "actor%i" numnodes) nodePushSum)
                    for i = 2 to numnodes - 1 do
                        if i = root then // Top right corner
                            neighMap <- neighMap.Add(i, [ i - 1; i + root ])
                        elif i + root - 1 = numnodes then // Bottom Left Corner
                            neighMap <- neighMap.Add(i, [ i - root; i + 1 ])
                        elif i < root then //First row
                            neighMap <- neighMap.Add(i, [ i - 1; i + 1; i + root ])
                        elif i + root - 1 > numnodes then //Last row
                            neighMap <- neighMap.Add(i, [ i - 1; i + 1; i - root ])
                        elif i % root = 0 then //Last column
                            neighMap <- neighMap.Add(i, [ i - root; i + root; i - 1 ])
                        elif (i - 1) % root = 0 then //First column
                            neighMap <- neighMap.Add(i, [ i - root; i + root; i + 1 ])
                        else //Center nodes
                            neighMap <- neighMap.Add(i, [ i + root; i - root; i - 1; i + 1 ])
                        nodeMap <- nodeMap.Add(i, spawn system (sprintf "actor%i" i) nodePushSum)
                    nodeMap.[startNode]
                    <! Activate1(startNode, 0.0, 0.0)
                else
                    printfn "Incrorrect Parameters."
                    flag <- true

            | Completion (actor) ->
                completedActors <- completedActors.Add(actor)
                //printfn "Number of converged actors are %d" completedActors.Count
                if float completedActors.Count
                   / float numnodes
                   >= 0.8 then
                    flag <- true

            | _ -> ()

            return! loop ()
        }

    loop ()

let start (n: int, a: string) =
    root <- int (ceil (sqrt (float n)))
    numnodes <- root * root
    let start = Random().Next(1, n + 1)
    let stopwatch = System.Diagnostics.Stopwatch.StartNew()
    spawnerId <- spawn system "spawner" spawner
    spawnerId <! Activate(numnodes, start, a)
    let mutable y = true
    while y do
        if flag then y <- false
    stopwatch.Stop()
    printfn "Total Time: %f ms" stopwatch.Elapsed.TotalMilliseconds
