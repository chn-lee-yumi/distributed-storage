package dss

/*
本文件包含了一些杂七杂八的实用函数，方便打码
*/

import (
    "io"
    "os"
    "fmt"
    "crypto/sha1"
)

/*
获取文件大小
*/
func getFileSize(filepath string) uint64 {
    f, err := os.Open(filepath)
    if err != nil {
        fmt.Println("[ERROR]文件大小读取错误！",err)
        os.Exit(1)
    }
    file_size, err := f.Seek(0, os.SEEK_END);checkErr(err)
    return uint64(file_size)
}

/*
错误检查函数
*/
func checkErr(err interface{}){
    if err != nil {
        fmt.Println("[ERROR]发生致命错误！以下是错误描述：")
        fmt.Println(err)
        os.Exit(1)
    }
}

/*
对文件进行hash，算法使用sha1
*/
func hashFile(filePath string) []byte {
    f, err := os.Open(filePath);checkErr(err)
    defer f.Close()
    h := sha1.New()
    _, err = io.Copy(h, f);checkErr(err)
    return h.Sum(nil) //长度20Byte
}

/*
对字节数组进行hash，算法使用sha1
*/
func hashBytes(b []byte) []byte {
    h := sha1.New()
    h.Write(b)
    return h.Sum(nil) //长度20Byte
}

/*
当verbose参数为true时，打印日志
*/
func log(v ... interface{}){
    if *verbose {
        fmt.Println(v...)
    }
}

/*
判断目录或文件是否存在
*/
func isPathExists(path string)bool{
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}

/*
字符串截取
*/
func subString(str string, start, end int) string {
	rs := []rune(str)
	length := len(rs)

	if start < 0 || start > length {
		panic("start is wrong")
	}

	if end < start || end > length {
		panic("end is wrong")
	}

	return string(rs[start:end])
}

/*
数组去重
*/
func RemoveDuplicatesAndEmpty(a []string) (ret []string){
    a_len := len(a)
    for i:=0; i < a_len; i++{
        if (i > 0 && a[i-1] == a[i]) || len(a[i])==0{
            continue;
        }
        ret = append(ret, a[i])
    }
    return
}
