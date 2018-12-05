/*
分布式文件共享系统



*/

package main

import (
    "fmt"
    "io/ioutil"
    "net"
    "time"
    "strings"
    "os"
    "bytes"
    "encoding/binary"
    "encoding/hex"
    "crypto/sha1"
    "io"
    "flag"
    "sync"
    "path/filepath"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "strconv"
    "errors"
)

const ( //定义指令码，数据包第一个字节为指令码
    DOWNLOAD_FILE byte = 1 //下载文件，后面跟文件的key
    //FILE_SIZE byte = 2 //文件大小，后面跟文件大小（单位Byte）
    LEND_GLOBAL_DB_LOCK byte = 6 //借用全局数据库锁
    RETURN_GLOBAL_DB_LOCK byte = 7 //归还全局数据库锁
    ACK byte = 8 //表示收到信息
    SYNC_GLOBAL_DB byte = 9 //同步数据库指令，后面跟文件大小（uint64）和数据库内容
    UPLOAD_FILE byte = 10 //上传文件指令，后面跟文件的key+文件大小+文件内容
    DELETE_FILE byte = 11 //删除文件指令，后面跟文件的key
    SEND_GLOBAL_DB byte = 12 //发送全局数据库指令
    JOIN_CLUSTER byte = 13 //加入集群指令，后面跟服务器端口（uint16）
    GET_SERVER_LIST byte = 14 //下载服务器列表
    SYNC_SERVER_LIST byte = 15 //同步服务器列表
)

const CLIENT_SHELL_HELP_MSG= //客户端命令行帮助信息
    `
    help：查看帮助
    ls：查看可下载的文件列表
        使用-l参数可以查看可下载的文件及其分块、分块所在的服务器
    get [filename]：下载文件
    upload [filename]：上传文件
    rm [filename]：删除文件
    exit：退出
    `

const FILE_BLOCK_SIZE=1024*1024*32 //文件分块大小，单位Byte


/*
数据库同步方案：
需要写入数据库的节点先给所有节点发送通知，申请分布式锁
节点写入完成后，将数据库同步给所有节点
同步完成后释放分布式锁
如果要读取数据，需要在没有节点写入数据库的时候读取（如果读取时有节点要申请锁，读取完再给）

数据库结构：
(
filename varchar(255),//文件名
num int(4),//文件分块号，从0开始
key char(40),//key，即文件分块名，sha1字符串形式，共40字节
server varchar(255) //文件分块所在的服务器
)

r, err := db.Exec(`CREATE TABLE keys(
filename varchar(255),
num int(4),
key char(40),
server varchar(255)
)`)
fmt.Println(r,err)

*/

//TODO:网络差的情况下可能不会接收到指令码之类的（需要将conn.Read改为阻塞或者用缓冲区接收）
//TODO:完成高可用
//TODO:文件重复上传的问题
//TODO:美化输出

var download_mission sync.WaitGroup //下载任务的WaitGroup
var server_list [] string //服务器列表，格式如“127.0.0.1::2333”
var global_db_lock_status int = FREE //全局数据库锁

const ( //定义全局数据库锁状态
    FREE = 0 //没有节点在使用全局数据库锁
    READING = 1 //本节点在读取全局数据库
    LOAN = 2 //锁已借出
    USING = 3 //锁自己在用
    /*
    全局数据库锁使用思路：
    如果需要写入，则等待global_db_lock_status=FREE，
    然后向所有节点发出申请，等待回复后global_db_lock_status=USING，
    写入完成后向所有节点进行同步，同步完成后向所有节点释放锁，并global_db_lock_status=FREE。
    如果需要读取，则等待global_db_lock_status=FREE，
    然后global_db_lock_status=READING，读取完毕后global_db_lock_status=FREE。
    如果有服务器要借用锁，则等待global_db_lock_status=FREE，
    然后借用，global_db_lock_status=LOAN，还回来后global_db_lock_status=FREE。
    TODO：考虑一下两个节点同时借用的死锁问题。
    */
)

var enable_server = flag.Bool("enable_server", false, "Enable server.启用服务器。")
var port = flag.String("port", "2333", "Listening port.监听端口（启用服务器才有效）。")
var first_server = flag.Bool("first_server", false, "First server, disable server scan.集群首台服务器，不进行服务器列表扫描。")
var verbose = flag.Bool("v", true, "Verbose output.输出详细信息。")

func main() {
    flag.Parse()//读取命令行参数
    log("enable_server",*enable_server)
    log("port",*port)
    log("verbose",*verbose)

    //根据参数判断是否作为服务端启动
    if *enable_server {
        fmt.Println("[INFO]系统启动……")
        go tcpServer(*port)//启动服务器，接收客户端和其它服务器的消息
    }

    if !*first_server {
        refreshServerList()//刷新服务器列表
        //连接服务器
        fmt.Println("[INFO]连接服务器……")
        var connected_server string
        for _,server:= range server_list {
            tcpAddr, _ := net.ResolveTCPAddr("tcp", server)
            conn, err := net.DialTCP("tcp", nil, tcpAddr)
            if err == nil {
                fmt.Println("服务器连接成功：",server)
                connected_server=server
                fmt.Println("[INFO]更新共享文件数据……")
                sendInstruct(SEND_GLOBAL_DB,conn)
                reciveFile("global.db",conn)//下载文件
                //加入服务器集群
                if *enable_server {
                    fmt.Println("[INFO]加入服务器集群……")
                    bytes_buf := bytes.NewBuffer(make([]byte, 0))
                    binary.Write(bytes_buf, binary.BigEndian, JOIN_CLUSTER)//1字节指令码
                    server_port, _ := strconv.ParseInt(*port, 10, 32)
                    binary.Write(bytes_buf, binary.BigEndian, uint16(server_port))//2字节端口号（uint16）
                    conn.Write(bytes_buf.Bytes())
                    instruct := readInstruct(conn)
                    if instruct==ACK {
                        fmt.Println("[INFO]服务器集群加入成功")
                    }else{
                        fmt.Println("[ERROR]服务器集群加入失败，请检查端口映射")
                    }
                }
                //关闭连接并退出循环
                conn.Close()
                break
            }
        }
        if connected_server=="" {
            if *enable_server {

            }else{
                fmt.Println("没有连上任何服务器，请检查服务器列表！")
                return
            }
        }

        fmt.Println("[INFO]更新服务器列表……")
        tcpAddr, _ := net.ResolveTCPAddr("tcp", connected_server)
        conn, err := net.DialTCP("tcp", nil, tcpAddr)
        if err != nil {
            fmt.Println("[ERROR]更新服务器列表失败，请检查网络连接！")
            os.Exit(1)
        }
        sendInstruct(GET_SERVER_LIST,conn)
        reciveFile("server_list.txt",conn)//下载文件
        refreshServerList()//刷新服务器列表
        conn.Close()
    }

    if *enable_server {
        fmt.Println("[INFO]服务器启动完成。")
    }else{
        go clientShell()//启用客户端命令行
    }

    for{
        time.Sleep(time.Hour)//死循环，任务交由其它goroutine执行
    }
}




func tcpServer(port string){//服务器goroutine，接收客户端和其它服务器的消息
    //启动服务器
    tcpAddr, err := net.ResolveTCPAddr("tcp",":"+port)
    tcpListener, err := net.ListenTCP("tcp",tcpAddr)
    if err != nil {
        fmt.Println("[ERROR]服务器启动错误：",err)
        panic("服务器启动错误")
    }
    //处理客户端传入连接
    ConnMap := make(map[string]*net.TCPConn)//使用Map来存储连接
    for{
        tcpConn, _ := tcpListener.AcceptTCP()
        defer tcpConn.Close()
        ConnMap[tcpConn.RemoteAddr().String()] = tcpConn
        fmt.Println("新的连接：",tcpConn.RemoteAddr().String())
        go clientHandle(tcpConn) //新建一个goroutine来处理客户端连接
    }
}


func clientHandle(conn net.Conn) {//客户端连接处理goroutine，处理客户端消息
    defer conn.Close() //函数结束前关闭连接
    defer fmt.Println("连接断开：",conn.RemoteAddr().String()) //函数结束前输出提示
    //循环的处理客户的请求
    for {
        conn.SetReadDeadline(time.Now().Add(time.Second * 5)) //一定时间内客户端无数据发送则关闭连接
        //读取数据
        //instruct := readInstruct(conn)
        instruct := make([]byte, 1) //初始化缓冲区，1字节
        _,err:=conn.Read(instruct)
        if err != nil {
            break
        }
        switch instruct[0] {//根据指令码做出选择
            case DOWNLOAD_FILE://下载文件
                key:=readKey(conn)//读取文件key
                log("[接收到指令]客户端下载文件：",key)
                sendFile("storage\\"+key,conn)//发送文件
                /*
                文件下载交互流程：
                客户端连接服务端
                客户端发送指令DOWNLOAD_FILE+文件key
                服务端发送文件大小（文件不存在则返回0）+文件内容
                客户端接收文件，直到接收到文件大小
                客户端关闭连接
                服务端关闭连接
                */
            case LEND_GLOBAL_DB_LOCK://借用全局数据库锁
                log("[接收到指令]借出全局数据库锁")
                acquireGlobalLock_Loan()
                sendInstruct(ACK,conn)
            case RETURN_GLOBAL_DB_LOCK://归还全局数据库锁
                log("[接收到指令]归还全局数据库锁")
                releaseGlobalLock_Loan()
                sendInstruct(ACK,conn)
            case SYNC_GLOBAL_DB://同步全局数据库
                log("[接收到指令]开始同步全局数据库")
                reciveFile("global.db",conn)
                sendInstruct(ACK,conn)
                fmt.Println("全局数据库同步完毕")
            case SYNC_SERVER_LIST://同步服务器列表
                log("[接收到指令]开始同步服务器列表")
                reciveFile("server_list.txt",conn)
                sendInstruct(ACK,conn)
                refreshServerList()
                fmt.Println("服务器列表同步完毕")
            case UPLOAD_FILE:
                key := readKey(conn)//读取文件key
                log("[接收到指令]客户端上传文件：",key)
                reciveFile("storage\\"+key,conn)
                sendInstruct(ACK,conn)
                fmt.Println("客户端文件上传完毕")
                /*
                文件上传交互流程：
                客户端连接服务端
                客户端发送指令UPLOAD_FILE+文件key+文件大小+文件内容
                服务端接收文件，直到接收到文件大小，返回ACK
                客户端关闭连接
                服务端关闭连接
                */
            case DELETE_FILE:
                key := readKey(conn)
                log("[接收到指令]客户端删除文件：",key)
                os.Remove("storage\\"+key)
                sendInstruct(ACK,conn)
                /*
                文件删除交互流程：
                客户端连接服务端
                客户端发送指令DELETE_FILE+文件key
                服务端删除文件，返回ACK
                客户端关闭连接
                服务端关闭连接
                */
            case SEND_GLOBAL_DB:
                log("[接收到指令]发送全局数据库")
                acquireGlobalLock_Read()
                sendFile("global.db",conn)
                releaseGlobalLock_Read()
                /*
                发送全局数据库交互流程：
                客户端连接服务端
                客户端发送指令SEND_GLOBAL_DB
                服务端返回文件大小（8字节）+全局数据库
                客户端关闭连接
                服务端关闭连接
                */
            case JOIN_CLUSTER:
                /*
                加入集群交互流程：
                客户端连接服务端
                客户端发送指令JOIN_CLUSTER+端口号
                服务端尝试连接，如果连接成功，更新服务器列表，返回ACK，否则返回0
                客户端关闭连接
                服务端关闭连接
                */
                log("[接收到指令]有服务器加入集群")
                //读取服务器端口
                data := make([]byte, 2)
                conn.Read(data)
                var server_port uint16
                binary.Read(bytes.NewBuffer(data), binary.BigEndian, &server_port)
                server:=strings.Split(conn.RemoteAddr().String(),":")[0]+":"+strconv.Itoa(int(server_port))
                //建立测试连接
                tcpAddr, _ := net.ResolveTCPAddr("tcp", server)
                test_conn, err := net.DialTCP("tcp", nil, tcpAddr)
                if err != nil {
                    test_conn.Close()
                    fmt.Println("测试连接失败")
                    conn.Write([]byte{255})
                    continue //结束处理
                }
                fmt.Println("测试连接成功")
                test_conn.Close()
                //更新服务器列表
                file_datas, _ := ioutil.ReadFile("server_list.txt")
                if !strings.Contains(string(file_datas),server){
                    fmt.Println("新增服务器：",server)
                    file_server_list_strings:=strings.TrimSpace(string(file_datas))+"\r\n"+server
                    file_server_list, _ := os.Create("server_list.txt")
                    file_server_list.Write([]byte(file_server_list_strings))
                    file_server_list.Close()
                }else{
                    fmt.Println("服务器已在列表中：",server)
                }
                sendInstruct(ACK,conn)//返回ACK
                syncServerList()//向所有节点同步服务器列表
                refreshServerList()//刷新服务器列表
            case GET_SERVER_LIST:
                log("[接收到指令]请求同步服务器列表")
                sendFile("server_list.txt",conn)
                /*
                同步服务器列表交互流程：
                客户端连接服务端
                客户端发送指令GET_SERVER_LIST
                服务端发送文件大小（8字节）+服务器列表
                客户端关闭连接
                服务端关闭连接
                */
            case 255:
        }
    }
}


func clientShell(){//客户端命令行
    fmt.Println("启用客户端命令行，输入help可以查看帮助。")
    for{
        fmt.Printf("$ ")
        var command string
        var parameter [3] string
        fmt.Scanf("%s %s %s %s", &command, &parameter[0], &parameter[1], &parameter[2])
        //fmt.Printf("%s %s %s %s\n", command, parameter[0], parameter[1], parameter[2])
        switch command {
            case "help"://帮助
                fmt.Println(CLIENT_SHELL_HELP_MSG)
            case "exit"://退出
                os.Exit(0)
            case "get"://下载文件
                /*
                下载文件流程：
                先从文件数据库查询文件名对应的文件分块数量和校验码
                然后根据文件分块所在的服务器，智能选择每个分块的下载服务器
                所有文件分块下载完成后，合并成一个完整文件
                */
                //先从文件数据库读取文件名对应的key和服务器
                fmt.Println("查找数据库……")
                acquireGlobalLock_Read()
                db, _ := sql.Open("sqlite3", "./global.db")//连接全局数据库
                //从数据库中读取文件名并新建下载任务
                rows, _ := db.Query("SELECT num,key,server FROM keys WHERE filename='"+parameter[0]+"' ORDER BY num")
                var key_list [] string
                for rows.Next() {
                    var key,server string
                    var num int
                    rows.Scan(&num,&key,&server)
                    fmt.Println(num,key,server)
                    download_mission.Add(1)
                    //TODO:处理多副本的情况，智能选择服务器
                    fmt.Println("提交下载任务",num)
                    key_list=append(key_list,key)
                    downloadFile(key, server)//TODO:高可用
                }
                rows.Close()
                db.Close()
                releaseGlobalLock_Read()
                //等待下载完毕
                fmt.Println("等待下载完成……")
                download_mission.Wait()
                //合并文件
                fmt.Println("合并文件块……")
                file_full, _ := os.Create("download\\"+parameter[0])
                for _,key := range key_list {
                    file_piece, _ := os.Open("tmp\\"+key)
                    buf:=make([]byte, getFileSize("tmp\\"+key))
                    file_piece.Read(buf)//全部读取
                    file_full.Write(buf)
                    file_piece.Close()
                    os.Remove("tmp\\"+key)
                }
                file_full.Close()
                fmt.Println("文件下载成功")
            case "ls"://查看可下载的文件列表
                acquireGlobalLock_Read()
                db, _ := sql.Open("sqlite3", "./global.db")//连接全局数据库
                //直接从数据库中读取文件名并打印
                if parameter[0]=="-l" {
                    rows, _ := db.Query("SELECT * FROM keys")
                    for rows.Next() {
                        var filename,key,server string
                        var num int
                        rows.Scan(&filename,&num,&key,&server)
                        fmt.Println(filename,num,key,server)
                    }
                    rows.Close()
                }else{
                    rows, _ := db.Query("SELECT DISTINCT filename FROM keys")
                    for rows.Next() {
                        var filename string
                        rows.Scan(&filename)
                        fmt.Println(filename)
                    }
                    rows.Close()
                }
                db.Close()
                releaseGlobalLock_Read()
            case "upload"://上传文件
                /*
                上传文件流程：
                判断文件大小，如果超过分块数量，则切割成块
                计算所有分块的sha1值并重命名
                选择服务器（负载均衡或者hash的方式）并上传文件块
                全部上传完毕后申请全局数据库锁
                将文件信息写入全局数据库
                释放全局数据库锁
                */
                file_path:=parameter[0]
                fmt.Println("文件路径：",file_path)
                _ , filename := filepath.Split(file_path)
                fmt.Println("文件名：",filename)
                file_size:=getFileSize(file_path)
                fmt.Println("文件大小：",file_size)
                var key_list [] string //key数组
                if file_size>FILE_BLOCK_SIZE{
                    file_pieces_num:=int(file_size/FILE_BLOCK_SIZE)+1
                    fmt.Println("文件超过文件块大小，需要分块。分块数量：",file_pieces_num)
                    f, _ := os.Open(file_path)
                    buf_size:=FILE_BLOCK_SIZE
                    for i:=0;i<file_pieces_num;i++ {
                        if int(file_size)-FILE_BLOCK_SIZE*i<FILE_BLOCK_SIZE {
                            buf_size=int(file_size)-FILE_BLOCK_SIZE*i
                        }
                        //fmt.Println(buf_size)
                        buf:=make([]byte, buf_size)
                        f.ReadAt(buf,int64(FILE_BLOCK_SIZE*i))//每次读取一个FILE_BLOCK_SIZE
                        key:=hex.EncodeToString(sha1Bytes(buf))//计算key
                        fmt.Println("第",i,"个key：",key)
                        key_list=append(key_list,key)
                        //写入文件
                        file_piece, _ := os.Create("tmp\\"+key)
                        file_piece.Write(buf)
                        file_piece.Close()
                    }
                    f.Close()
                    fmt.Println("文件分块完成！")
                }else{
                    fmt.Println("文件小于等于文件块大小，无需分块。")
                    f, _ := os.Open(file_path)
                    buf:=make([]byte, file_size)
                    f.Read(buf)//全部读取
                    key:=hex.EncodeToString(sha1Bytes(buf))//计算key
                    fmt.Println("第0个key：",key)
                    key_list=append(key_list,key)
                    //写入文件
                    file_piece, _ := os.Create("tmp\\"+key)
                    file_piece.Write(buf)
                    file_piece.Close()
                    f.Close()
                }
                //选择服务器并上传文件块
                //查询数据库，计算每个服务器的文件数量，从小到大排序，排序相同的按服务器字符串排序
                //将一个分块发送到一个服务器上，然后重复上面的步骤，查询最佳服务器并继续上传
                fmt.Println("准备上传文件分块……")
                for i,key := range key_list {
                    //选择服务器 TODO：性能待优化
                    fmt.Println("查找数据库……")
                    acquireGlobalLock_Read()
                    db, _ := sql.Open("sqlite3", "./global.db")
                    var min_key_nums int = -1 //服务器集群的最小key数
                    var min_key_nums_server string //服务器集群的最小key数所在的服务器
                    for _,server := range server_list {
                        var num int
                        db.QueryRow("SELECT COUNT(*) FROM keys WHERE server='"+server+"'").Scan(&num)
                        fmt.Println(server,num)
                        if min_key_nums==-1 || num<min_key_nums {
                            min_key_nums=num
                            min_key_nums_server=server
                        }
                    }
                    db.Close()
                    releaseGlobalLock_Read()
                    fmt.Println("最佳上传服务器：",min_key_nums_server,min_key_nums)
                    //连接服务器
                    tcpAddr, _ := net.ResolveTCPAddr("tcp", min_key_nums_server)
                    conn, err := net.DialTCP("tcp", nil, tcpAddr)
                    if err != nil {//TODO:高可用
                        fmt.Println("服务器连接失败：",tcpAddr)
                    }
                    //上传文件分块
                    fmt.Println("上传第",i,"个文件分块：",key)
                    sendInstruct(UPLOAD_FILE,conn)
                    sendString(key,conn)
                    sendFile("tmp\\"+key,conn)
                    //等待服务端回应ACK
                    instruct:=readInstruct(conn)
                    if instruct==ACK {
                        fmt.Println("第",i,"个文件分块上传成功")
                    }
                    conn.Close()
                    //删除文件
                    err = os.Remove("tmp\\"+key)
                    if err!=nil {
                        fmt.Println("[ERROR]文件删除失败，可稍后手动删除。",err)
                    }
                    //写入全局数据库
                    fmt.Println("准备写入全局数据库……")
                    //申请全局数据库锁，TODO：避免死锁
                    acquireGlobalLock_Write()
                    //写入数据库
                    db, _ = sql.Open("sqlite3", "./global.db")//连接全局数据库
                    stmt, _ := db.Prepare("INSERT INTO keys VALUES (?,?,?,?)")
                    fmt.Println("插入数据：",filename,i,key,min_key_nums_server)
                    stmt.Exec(filename,i,key,min_key_nums_server)
                    db.Close()
                    fmt.Println("数据库更新成功。")
                    //同步数据库到其它服务器
                    syncGlobalDatabase()
                    //归还全局数据库锁
                    releaseGlobalLock_Write()
                }
                fmt.Println("文件上传完毕！")
            case "rm"://删除文件
                /*
                删除文件流程：
                申请全局数据库锁
                将文件信息从全局数据库中删除
                释放全局数据库锁
                通知对应的服务器删除文件块
                */
                fmt.Println("准备写入全局数据库……")
                //申请全局数据库锁，TODO：避免死锁
                acquireGlobalLock_Write()
                //查询key并删除
                db, _ := sql.Open("sqlite3", "./global.db")//连接全局数据库
                rows, _ := db.Query("SELECT key,server FROM keys WHERE filename='"+parameter[0]+"'")
                var key_list [] string
                for rows.Next() {
                    var key,server string
                    rows.Scan(&key,&server)
                    fmt.Println(key,server)
                    key_list=append(key_list,key)
                }
                rows.Close()
                db.Exec("DELETE FROM keys WHERE filename = '"+parameter[0]+"'")//删除文件
                db.Close()
                fmt.Println("数据库更新成功。")
                //同步数据库到其它服务器
                syncGlobalDatabase()
                //归还全局数据库锁
                releaseGlobalLock_Write()
                //通知对应的服务器删除文件块
                for _,key := range key_list {
                    bytes_buf := bytes.NewBuffer(make([]byte, 0))
                    binary.Write(bytes_buf, binary.BigEndian, DELETE_FILE)
                    binary.Write(bytes_buf, binary.BigEndian, []byte(key))
                    sendDatasToAllServers(bytes_buf.Bytes())
                }
                fmt.Println("文件删除完毕！")
        }
    }
}























//sha1File利用sha1算法将目标文件生成哈希值 https://blog.csdn.net/benben_2015/article/details/80146147
func sha1File(filePath string) []byte {
    f, err := os.Open(filePath)
    if err != nil {
        fmt.Println(err)
    }
    defer f.Close()

    h := sha1.New()
    if _, err := io.Copy(h, f); err != nil {
        fmt.Println(err)
    }
    //fmt.Println(hex.EncodeToString(h.Sum(nil)))
    return h.Sum(nil) //长度20Byte
}

func sha1Bytes(b []byte) []byte {
    h := sha1.New()
    h.Write(b)
    //fmt.Println(hex.EncodeToString(h.Sum(nil)))
    return h.Sum(nil) //长度20Byte
}

func getFileSize(filepath string) uint64 {//读取文件大小
    f, err := os.Open(filepath)
    if err != nil {
        fmt.Println("[ERROR]文件大小读取错误！",err)
        return 0
    }
    file_size, _  := f.Seek(0, os.SEEK_END)
    return uint64(file_size)
}

func downloadFile(filename string, tcpAddrString string)error{//下载文件
    //连接服务器
    tcpAddr, _ := net.ResolveTCPAddr("tcp", tcpAddrString)
    conn, err := net.DialTCP("tcp", nil, tcpAddr)
    if err != nil {
        fmt.Println("服务器连接失败")
        return errors.New("服务器连接失败")
    }
    defer conn.Close()
    //发送下载请求
    sendInstruct(DOWNLOAD_FILE,conn)
    sendString(filename,conn)
    //下载文件
    reciveFile("tmp\\"+filename,conn)
    //完成任务
    download_mission.Done()
    return nil
}

func sendDatasToAllServers(datas []byte){
    for _, server := range server_list{
        tcpAddr, _ := net.ResolveTCPAddr("tcp", server)
        conn, err := net.DialTCP("tcp", nil, tcpAddr)
        if err != nil {
            fmt.Println("服务器连接失败：",tcpAddr)
            continue
        }
        conn.Write(datas)
        //fmt.Println("数据发送成功：",datas)
        instruct := make([]byte, 1)
        conn.Read(instruct)
        //fmt.Println("数据读取成功：",instruct)
        var i byte
        binary.Read(bytes.NewBuffer(instruct), binary.BigEndian, &i)
        for{
            if i==ACK {
                break
            }
            time.Sleep(time.Millisecond*10)
        }
        conn.Close()
    }
}

func acquireGlobalLock_Write(){//请求全局数据库锁
    for{
        if global_db_lock_status==FREE{
            global_db_lock_status=USING
            sendDatasToAllServers([]byte{LEND_GLOBAL_DB_LOCK})
            fmt.Println("全局数据库锁申请成功。")
            break
        }
    }
}

func releaseGlobalLock_Write(){//释放全局数据库锁
    sendDatasToAllServers([]byte{RETURN_GLOBAL_DB_LOCK})
    global_db_lock_status=FREE //=releaseGlobalLock_Read()
    fmt.Println("全局数据库锁归还成功。")
}

func acquireGlobalLock_Read(){//请求全局数据库读取锁
    for{
        if global_db_lock_status==FREE{
            global_db_lock_status=READING
            break
        }
    }
}

func releaseGlobalLock_Read(){//释放全局数据库读取锁
    global_db_lock_status=FREE
}

func acquireGlobalLock_Loan(){//请求全局数据库锁，借出
    for{
        if global_db_lock_status==FREE{
            global_db_lock_status=LOAN
            break
        }
    }
}

func releaseGlobalLock_Loan(){//释放全局数据库锁(借出)
    global_db_lock_status=FREE
}

func syncGlobalDatabase(){
    bytes_buf := bytes.NewBuffer(make([]byte, 0))
    binary.Write(bytes_buf, binary.BigEndian, SYNC_GLOBAL_DB)
    binary.Write(bytes_buf, binary.BigEndian, getFileSize("global.db"))
    file_datas, _ := ioutil.ReadFile("global.db")
    binary.Write(bytes_buf, binary.BigEndian, file_datas)
    sendDatasToAllServers(bytes_buf.Bytes())
    fmt.Println("数据库同步成功。")
}

func syncServerList(){
    bytes_buf := bytes.NewBuffer(make([]byte, 0))
    binary.Write(bytes_buf, binary.BigEndian, SYNC_SERVER_LIST)
    binary.Write(bytes_buf, binary.BigEndian, getFileSize("server_list.txt"))
    file_datas, _ := ioutil.ReadFile("server_list.txt")
    binary.Write(bytes_buf, binary.BigEndian, file_datas)
    sendDatasToAllServers(bytes_buf.Bytes())
    fmt.Println("数据库同步成功。")
}

func log(v ... interface{}){
    if *verbose {
        fmt.Println(v...)
    }
}

func refreshServerList(){//刷新服务器列表
    //读取服务器列表
    log("读取服务器列表")
    b, err := ioutil.ReadFile("server_list.txt")
    if err != nil {
        fmt.Print(err)
    }
    //将文件内容转为字符串，去除首尾的空白字符，按换行切割（如果换行是linux，只要\n），结果为服务器IP数组
    server_list = strings.Split(strings.TrimSpace(string(b)), "\r\n")
    for i,server:= range server_list {
        fmt.Printf("服务器%d：%s\n", i,server)
    }
}

func reciveFile(file_path string, conn net.Conn){
    data := make([]byte, 8)
    conn.Read(data)
    var file_size uint64
    binary.Read(bytes.NewBuffer(data), binary.BigEndian, &file_size)
    log("文件大小：",file_size)
    var download_size uint64 = 0
    var buf_size=1024*1024*4 //4M，这个大小对结果无影响
    f, _ := os.Create(file_path)
    for{
        data := make([]byte, buf_size)
        n, _ := conn.Read(data)
        f.Write(data[:n])
        download_size+=uint64(n)
        fmt.Printf("进度：%.2f\n",float32(download_size)*100/float32(file_size))
        if download_size==file_size{
            break
        }
    }
    f.Close()
    fmt.Println("文件下载完毕")
}

func sendFile(file_path string, conn net.Conn){
    bytes_buf := bytes.NewBuffer(make([]byte, 0))
    //binary.Write(bytes_buf, binary.BigEndian, FILE_SIZE)//1字节指令码
    binary.Write(bytes_buf, binary.BigEndian, getFileSize(file_path))//8字节文件大小（uint64）
    conn.Write(bytes_buf.Bytes())
    //发送文件 TODO：对大文件的优化。目前是全部读进内存。
    log("开始发送文件……")
    file_datas, _ := ioutil.ReadFile(file_path)
    conn.Write(file_datas)
    log("文件发送完毕！")//客户端接收完成后会关闭连接，服务器会自动关闭
}

func readInstruct(conn net.Conn)byte{
    instruct := make([]byte, 1) //初始化缓冲区，1字节
    for {
        n, err := conn.Read(instruct) //从conn中读取数据，n为数据大小
        if err != nil && err.Error() != "EOF" {
            fmt.Println("[ERROR]读取指令出错：",err)
            return 255
            //panic("读取指令出错")
        }
        if n!=0 {
            break
        }
        time.Sleep(time.Millisecond*10)
    }
    return instruct[0]
}

func sendInstruct(instruct byte,conn net.Conn){
    conn.Write([]byte{instruct})
}

func sendString(strings string,conn net.Conn){
    conn.Write([]byte(strings))
}

func readKey(conn net.Conn)string{
    data := make([]byte, 40)//key长度40
    n, _ :=conn.Read(data)//读取key
    key:=string(data[0:n])//将key转成字符串
    return key
}
