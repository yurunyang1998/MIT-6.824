package main
import "io/ioutil"
import "fmt"
import "strconv"
import "strings"

func main(){
	filesnames, err := ioutil.ReadDir("./")
	if(err!=nil){
		fmt.Println(err)
	}
	targetFiles := make([]string, 10)
	for _, file := range(filesnames){

		splitWords := strings.Split(file.Name(),"_")
		if len(splitWords)==0{
			print(splitWords)
			continue
		}
		index, err := strconv.Atoi(splitWords[1])
		if err!=nil{
			print(err)
			continue
		}

		if(index == 8){
			targetFiles = append(targetFiles, file.Name())
			print(file.Name())
		}
	}

	for _, file := range(targetFiles){
		fmt.Println(file)
	}
}
