package dss

/*
本文件包含了集群连接相关的函数
*/

import (
    "fmt"
    "os"
    "net"
    "errors"
    "time"
    "bytes"
    "io"
    "io/ioutil"
    "strings"
    "encoding/binary"
)

/*
客户端下载文件
*/
func downloadFile(filename string, tcpAddrString string)error{
    //连接服务器
    conn, err := net.DialTimeout("tcp", tcpAddrString, NET_TIMEOUT)
    if err != nil {
        fmt.Println("服务器连接失败")
        return errors.New("服务器连接失败")
    }
    defer conn.Close()
    //发送下载请求
    sendInstruct(DOWNLOAD_FILE,conn)
    sendString(filename,conn)
    //下载文件
    err=reciveFile("tmp/"+filename,conn)
    if err!=nil {
        fmt.Println("[ERROR]下载文件失败")
        os.Exit(1)
    }
    //完成任务
    download_mission.Done()
    return nil
}

/*
向所有服务器发送相同的数据（相当于广播）
*/
func sendDatasToAllServers(datas []byte){
    for _, server := range global_server_list{
        if server == self_server_addr {continue}
        conn, err := net.DialTimeout("tcp", server, NET_TIMEOUT)
        if err != nil {
            fmt.Println("服务器连接失败：",server)
            continue
        }
        //log("向"+server+"发送指令：",datas)
        log("向",server,"发送了",len(datas),"字节的数据")
        conn.Write(datas)
        instruct := make([]byte, 1)
        conn.Read(instruct)
        var i byte
        binary.Read(bytes.NewBuffer(instruct), binary.BigEndian, &i)
        for{
            if i==ACK {
                log("收到"+server+"回复：ACK")
                break
            }
            time.Sleep(time.Millisecond*10)
        }
        conn.Close()
    }
}

/*
客户端上传数据库给服务器
*/
func uploadDatabase(){
    log("向其它服务器发送数据库……",username)
    compressUserDatabase()
    bytes_buf := bytes.NewBuffer(make([]byte, 0))
    binary.Write(bytes_buf, binary.BigEndian, SYNC_DB)
    binary.Write(bytes_buf, binary.BigEndian, getFileSize(DB_PATH))
    file_datas, err := ioutil.ReadFile(DB_PATH);checkErr(err)
    binary.Write(bytes_buf, binary.BigEndian, file_datas)
    sendDatasToAllServers(bytes_buf.Bytes())
    log("数据库同步成功。")
}

/*
服务器之间同步服务器列表
*/
func syncServerList(){
    log("向其它服务器发送服务器列表……")
    bytes_buf := bytes.NewBuffer(make([]byte, 0))
    binary.Write(bytes_buf, binary.BigEndian, SYNC_SERVER_LIST)
    binary.Write(bytes_buf, binary.BigEndian, getFileSize("server_list.txt"))
    file_datas, err := ioutil.ReadFile("server_list.txt");checkErr(err)
    binary.Write(bytes_buf, binary.BigEndian, file_datas)
    sendDatasToAllServers(bytes_buf.Bytes())
    log("服务器列表同步成功。")
}

/*
读取服务器列表文件server_list.txt到变量global_server_list中
*/
func refreshServerList(){
    //读取服务器列表
    log("读取服务器列表")
    b, err := ioutil.ReadFile("server_list.txt");checkErr(err)
    //将文件内容转为字符串，去除首尾的空白字符，按换行切割（如果换行是linux，只要\n），结果为服务器IP数组
    global_server_list = strings.Split(strings.TrimSpace(string(b)), "\r\n")
    for i,server:= range global_server_list {
        fmt.Printf("服务器%d：%s\n", i,server)
    }
}

/*
接收文件
*/
func reciveFile(file_path string, conn net.Conn)error{
    time_start:=time.Now()
    //time.Sleep(time.Millisecond * 200)//以防接不到数据
    data := make([]byte, 8)
    conn.Read(data)
    var file_size uint64
    binary.Read(bytes.NewBuffer(data), binary.BigEndian, &file_size)
    log("文件大小：",file_size)
    var download_size uint64 = 0
    f, err := os.Create(file_path);checkErr(err)
    defer f.Close()
    for{
        data := make([]byte, FILE_READ_SIZE)
        n, err := conn.Read(data);
        if err != nil {
            fmt.Println("[WARN]文件下载出错",err)
            return err
        }
        f.Write(data[:n])
        download_size+=uint64(n)
        fmt.Printf("进度：%.2f\n",float32(download_size)*100/float32(file_size))//TODO:减缓输出速度
        if download_size==file_size{
            break
        }
    }
    fmt.Println("文件下载完毕")
    time_end:=time.Now()
    fmt.Printf("下载速度：%.3f MB/s\n",float64(float64(file_size)/1024/1024/time_end.Sub(time_start).Seconds()))
    return nil
}

/*
发送文件
*/
func sendFile(file_path string, conn net.Conn){
    time_start:=time.Now()
    bytes_buf := bytes.NewBuffer(make([]byte, 0))
    file_size:=getFileSize(file_path)
    binary.Write(bytes_buf, binary.BigEndian, file_size)//8字节文件大小（uint64）
    conn.Write(bytes_buf.Bytes())
    //发送文件
    var upload_size uint64 = 0
    log("开始发送文件……")
    f,err:=os.Open(file_path);checkErr(err)
    defer f.Close()
    buf := make([]byte, FILE_READ_SIZE)
    for {
        n, err := f.Read(buf)
        if err != nil && err != io.EOF {
            fmt.Println("[WARN]文件发送出错",err)
            return
        }
        if 0 == n {
            break
        }
        writeAll(conn,buf[:n])
        upload_size+=uint64(n)
        fmt.Printf("进度：%.2f\n",float32(upload_size)*100/float32(file_size))
    }
    log("文件发送完毕！")//客户端接收完成后会关闭连接，服务器会自动关闭
    time_end:=time.Now()
    fmt.Printf("上传速度：%.3f MB/s\n",float64(float64(file_size)/1024/1024/time_end.Sub(time_start).Seconds()))
}

/*
读取指令
*/
func readInstruct(conn net.Conn)byte{
    instruct := make([]byte, 1) //初始化缓冲区，1字节
    _,err:=conn.Read(instruct)
    if err==io.EOF{
        return ERR
    }
    if err!=nil {
        fmt.Println("[ERROR]读取指令出错：",err)
        return ERR
    }
    return instruct[0]
}

/*
发送指令
*/
func sendInstruct(instruct byte,conn net.Conn){
    conn.Write([]byte{instruct})
}

/*
发送字符串
*/
func sendString(strings string,conn net.Conn){
    conn.Write([]byte(strings))
}

/*
读取Key
*/
func readKey(conn net.Conn)string{
    data := make([]byte, 40)//key长度40
    n, _ :=conn.Read(data)//读取key
    key:=string(data[0:n])//将key转成字符串
    return key
}

/*
客户端获取服务器负载
*/
func getServerLoad(server string)uint8{//获取服务器负载
    conn, err := net.DialTimeout("tcp", server, NET_TIMEOUT)
    if err != nil {
        fmt.Println("服务器连接失败：",server)
        return ERR
    }
    conn.Write([]byte{SERVER_LOAD})
    server_load := make([]byte, 1)
    conn.Read(server_load)
    conn.Close()
    return uint8(server_load[0])
}

/*
客户端上传文件
*/
func uploadFile(key string,conn net.Conn){
    sendInstruct(UPLOAD_FILE,conn)
    sendString(key,conn)
    sendFile("tmp/"+key,conn)
    instruct:=readInstruct(conn)//等待服务端回应ACK
    if instruct==ACK {
        fmt.Println("上传成功：",key)
    }else{
        fmt.Println("[ERROR]上传失败：",key)//TODO:处理失败情形
    }
    conn.Close()
    upload_mission.Done()
}

/*
写入全部字节
*/
func writeAll(c net.Conn, b []byte)error{
    for{
        if len(b) <= 0{
            break
        }
        tmpn, err := c.Write(b)
        if err != nil{
            return err
        }
        b = b[tmpn:]
    }
    return nil
}

/*
客户端获取最新到数据库
*/
func getGlobalDatabase(){
    log("获取最新数据库……")
    for _,server:= range global_server_list {
        conn, err := net.DialTimeout("tcp", server, NET_TIMEOUT)
        if err!=nil {continue}
        fmt.Println("服务器连接成功：",server)
        sendInstruct(SEND_DB,conn)
        err=reciveFile(DB_PATH,conn)//下载文件
        if err!=nil {
            fmt.Println("[ERROR]数据库下载失败")
            conn.Close()
            continue
        }
        decompressDatabase()
        //关闭连接并退出循环
        conn.Close()
        return
    }
    fmt.Println("[ERROR]数据库下载失败：没有可用的服务器。")
    os.Exit(1)
}

/*
客户端获取最新的服务器列表（服务器在加入集群之后也会调用一次）
*/
func updateServerList(){
    log("获取最新服务器列表……")
    for _,server:= range global_server_list {
        conn, err := net.DialTimeout("tcp", server, NET_TIMEOUT)
        if err!=nil {continue}
        fmt.Println("服务器连接成功：",server)
        sendInstruct(GET_SERVER_LIST,conn)
        err=reciveFile("server_list.txt",conn)
        if err!=nil {
            fmt.Println("[ERROR]服务器列表下载失败：",err)
            conn.Close()
            continue
        }
        //关闭连接并退出循环
        conn.Close()
        refreshServerList()//刷新服务器列表
        return
    }
    fmt.Println("[ERROR]服务器列表下载失败：没有可用的服务器。")
    os.Exit(1)
}
