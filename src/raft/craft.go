package main
import(
    "time"
    "fmt"
)

func main() {
    now := time.Now()
    for i:=0;i<100300;i++{

    }
    lastBeatHeartTime := time.Now()
    elaspe := now.Sub(lastBeatHeartTime()
    fmt.Printf("%d",elaspe.Milliseconds)
}