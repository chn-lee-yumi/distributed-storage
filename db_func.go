package dss

/*
本文件包含了数据库相关的函数
*/

/*
数据库结构：
TABEL key_server(
    key char(40),//key，即文件分块名，sha1字符串形式，共40字节
    server varchar(255) //文件分块所在的服务器
)
TABEL file_key(
    filename varchar(255),//文件名
    num int(4),//文件分块号，从0开始
    key char(40),//key，即文件分块名，sha1字符串形式，共40字节
)
*/

import (
    "os"
    "io/ioutil"
    "time"
)


/*
申请数据库锁
*/
func acquireGlobalLock(){
    log("等待数据库锁……")
    for{
        if global_db_lock_status==FREE {
            log("得到数据库锁。")
            global_db_lock_status=USING
            return
        }
        time.Sleep(time.Millisecond*10)//避免CPU占用100%
    }
}

/*
释放数据库锁
*/
func releaseGlobalLock(){
    log("释放数据库锁。")
    global_db_lock_status=FREE
}

/*
根据用户名取得数据库路径
*/
func dbPath(user string)string{
    return "database/"+user+".db"
}


/*
压缩用户数据库，客户端上传或删除文件后，更新数据库到集群时会用到
*/
func compressUserDatabase(){
    f1, err := os.Open(dbPath(username))//username为全局变量，当前登录用户名
	if err != nil {
		log(err)
        os.Exit(1)
	}
	defer f1.Close()
    var files = []*os.File{f1}
    err = Compress(files, DB_PATH)
	if err != nil {
		log(err)
        os.Exit(1)
	}
}

/*
压缩所有数据库，服务端发送数据库时会用到。
*/
func compressDatabase(){
    var files = []*os.File{}
    dir, err := ioutil.ReadDir("database");checkErr(err)
    for _,f := range dir {
        if(subString(f.Name(),0,1)=="."){continue}
        f1, err := os.Open("database/"+f.Name())
        if err != nil {
    		log(err)
            os.Exit(1)
    	}
        files=append(files,f1)
        defer f1.Close()
    }
    err = Compress(files, DB_PATH)
	if err != nil {
		log(err)
        os.Exit(1)
	}
}

/*
解压缩数据库，客户端或服务端接受数据库时会用到
*/
func decompressDatabase(){
    err := DeCompress(DB_PATH, "database")
	if err != nil {
		log(err)
        os.Exit(1)
	}
}
