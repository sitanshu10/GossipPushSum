#load "Full.fsx"
#load "Norm2D.fsx"
#load "Imp2D.fsx"
#load "Line.fsx"

let main () =
    let n = int fsi.CommandLineArgs.[1]
    let t = string fsi.CommandLineArgs.[2]
    let a = string fsi.CommandLineArgs.[3]

    if t = "full" then Full.start (n, a)
    elif t = "2D" then Norm2D.start (n, a)
    elif t = "imp2D" then Imp2D.start (n, a)
    elif t = "line" then Line.start (n, a)
    else printfn "Incrorrect Parameters."

main ()
