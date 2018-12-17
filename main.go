/*
分布式文件共享系统
*/

//TODO：磁盘空间 https://blog.csdn.net/webxscan/article/details/72857292
//TODO：双击运行，可选部署服务器或者客户端
//TODO：退出集群、数据库冗余项的清理、服务器列表废弃服务器的清理
//TODO：文件块下载完成进行校验，断点续传，迁移等功能，相同hash的分块不需要上传/重复删除等，美化输出

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
    "flag"
    "sync"
    "path/filepath"
    "database/sql"
    _ "modernc.org/ql/driver"
    "strconv"
    "sort"
    "github.com/remeh/sizedwaitgroup"
)

const ( //定义指令码，数据包第一个字节为指令码
    DOWNLOAD_FILE byte = 1 //下载文件，后面跟文件的key
    SEND_DB byte = 2 //发送数据库指令
    ACK byte = 8 //表示收到信息
    SYNC_DB byte = 9 //同步（接受）数据库指令，后面跟文件大小（uint64）和数据库内容
    UPLOAD_FILE byte = 10 //上传文件指令，后面跟文件的key+文件大小+文件内容
    DELETE_FILE byte = 11 //删除文件指令，后面跟文件的key
    JOIN_CLUSTER byte = 13 //加入集群指令，后面跟服务器端口（uint16）
    GET_SERVER_LIST byte = 14 //下载服务器列表
    SYNC_SERVER_LIST byte = 15 //同步服务器列表
    SERVER_LOAD byte = 16 //服务器负载
    ERR byte = 255 //错误
)

const (
    DB_TYPE="ql2" //数据库类型
    DB_PATH="tmp/db.zip" //数据库压缩文件路径
)
const ( //定义数据库锁状态
    FREE = 0
    USING = 1 //本节点在读取数据库
)

const CLIENT_SHELL_HELP_MSG= //客户端命令行帮助信息
    `
    help：查看帮助
    ls：查看可下载的文件列表
        使用-l参数可以查看可下载的文件及其分块、分块所在的服务器
    login [username]：登录，使用get命令和del命令时需要
    get [username] [filename]：下载文件
    put [filename]：上传文件
    del [filename]：删除文件
    update：更新数据库（客户端启动时也会自动更新）
    status：服务器状态
    exit：退出
    `

const CLIENT_SHELL_WELCOME_MSG= //客户端命令行欢迎信息
    `
    **************************************************
    注意事项：
    1. 上传或删除文件前请先使用login命令登录，用户名请每人固定下来，不要冲突，如果不确定名字有没有人用，可用ls命令查看。登录命令例子：login yumi。同一个用户请不要同时上传多个文件，否则会造成数据库损坏。
    2. 不同用户上传的文件名可以相同，但请不要上传同样的文件（文件块hash相同），否则删除时会一并删除文件块。（这个问题会在后续版本修复）
    3. 当前用户名可在命令行前缀查看，默认为Anonymous。下载文件不需要登录。
    4. 用户名和文件名请不要包含空格。
    **************************************************
    启用客户端命令行，欢迎使用GDUT-DistributeStorageSystem！
    输入help获取帮助。
    `

const FILE_BLOCK_SIZE=1024*1024*32 //文件分块大小，单位Byte
const FILE_READ_SIZE=1024*1024*2 //读取缓存大小
const NET_TIMEOUT=time.Millisecond*300

type KeyServerPair struct {//Key-服务器对
    Key string
    Server []string
}

var download_mission=sizedwaitgroup.New(2) //最大同时下载任务为2
var upload_mission sync.WaitGroup //上传任务的WaitGroup
var global_server_list [] string //服务器列表，格式如“127.0.0.1::2333”
var global_db_lock_status int = FREE //数据库锁
var global_server_load uint8 = 0 //服务器负载
var self_server_addr string
var username string = "Anonymous"

var enable_server = flag.Bool("enable_server", false, "Enable server.启用服务器。")
var port = flag.String("port", "2333", "Listening port.监听端口（启用服务器才有效）。")
var first_server = flag.Bool("first_server", false, "First server, disable server scan.集群首台服务器，不进行服务器列表扫描。")
var verbose = flag.Bool("v", true, "Verbose output.输出详细信息。")

func main() {

    flag.Parse()//读取命令行参数
    log("命令行参数：")
    log("enable_server",*enable_server)
    log("first_server",*first_server)
    log("port",*port)
    log("verbose",*verbose)

    //创建文件夹
    if(!isPathExists("tmp")){os.Mkdir("tmp", os.ModePerm)}
    if(!isPathExists("storage")){os.Mkdir("storage", os.ModePerm)}
    if(!isPathExists("download")){os.Mkdir("download", os.ModePerm)}
    if(!isPathExists("database")){os.Mkdir("database", os.ModePerm)}

    //根据参数判断是否作为服务端启动
    if !*first_server {
        fmt.Println("[INFO]读取服务器列表……")
        refreshServerList()
        fmt.Println("[INFO]更新服务器列表……")
        updateServerList()

        if !*enable_server {//如果是客户端
            fmt.Println("[INFO]更新共享文件数据……")
            getGlobalDatabase()
        }else{//如果是服务器
            fmt.Println("[INFO]系统启动……")
            fmt.Println("[INFO]连接服务器……准备加入集群")
            var connected_server string
            for _,server:= range global_server_list {
                conn, err := net.DialTimeout("tcp", server, NET_TIMEOUT)
                if err!=nil {continue}
                fmt.Println("服务器连接成功：",server)
                connected_server=server
                //加入服务器集群
                go testConn()//加入集群需启动一个测试连接服务端
                fmt.Println("[INFO]加入服务器集群……")
                bytes_buf := bytes.NewBuffer(make([]byte, 0))
                binary.Write(bytes_buf, binary.BigEndian, JOIN_CLUSTER)//1字节指令码
                server_port, err := strconv.ParseInt(*port, 10, 32);checkErr(err)
                binary.Write(bytes_buf, binary.BigEndian, uint16(server_port))//2字节端口号（uint16）
                conn.Write(bytes_buf.Bytes())
                instruct := readInstruct(conn)
                if instruct==ACK {
                    fmt.Println("[INFO]服务器集群加入成功")
                    //得到本机ip，更新数据库要用
                    data := make([]byte, 21)//地址长度最大21：xxx.xxx.xxx.xxx:xxxxx
                    n, _ :=conn.Read(data)//读取本机地址
                    self_server_addr=string(data[0:n])
                    fmt.Println("本机地址：",self_server_addr)
                }else{
                    fmt.Println("[ERROR]服务器集群加入失败，请检查端口映射")
                    os.Exit(1)
                }
                //关闭连接并退出循环
                conn.Close()
                break
            }
            if connected_server=="" {
                fmt.Println("[ERROR]准备加入集群时发现没有可以连接上的服务器。")
                os.Exit(1)
            }
            fmt.Println("[INFO]更新服务器列表……")//加入集群后再次更新
            time.Sleep(NET_TIMEOUT)//需要sleep，否则会卡住
            updateServerList()

            //TODO:查询本地的块，结合数据库，进行删除或添加
            //fmt.Println("[INFO]更新数据库文件……")
            //获取所有数据库的key
            /*key_list:=make(map[string]string)
            dir, err := ioutil.ReadDir("database");checkErr(err)
            for _,f := range dir {
                if(subString(f.Name(),0,1)=="."){continue}
                db, err := sql.Open(DB_TYPE, "database/"+f.Name());checkErr(err)
                rows, err := db.Query(`SELECT key FROM FileKey`);checkErr(err)
                for rows.Next() {
                    var key string
                    if err = rows.Scan(&key); err != nil {
                        rows.Close()
                        break
                    }
                    key_list[key]=""
                }
                err = db.Close();checkErr(err)
            }*/
            //删除本地冗余文件
            /*fmt.Println("[INFO]检查冗余文件……")
            dir, err = ioutil.ReadDir("storage");checkErr(err)
            for _,f := range dir {
                if(subString(f.Name(),0,1)=="."){continue}
                if _,exist := key_list[f.Name()];!exist {
                    log("发现废弃数据块。")
                    err := os.Remove("storage/"+f.Name())
                    if err==nil {
                        log("数据块删除成功：",f.Name())
                    }else{
                        log("数据块删除失败：",f.Name(),err)
                    }
                }
            }*/
            //TODO:添加已有的块
            /*dir, err = ioutil.ReadDir("storage");checkErr(err)
            for _,f := range dir {
                if(subString(f.Name(),0,1)=="."){continue}
                var key string
                db, err := sql.Open(DB_TYPE, "database/"+f.Name());checkErr(err)
                db.QueryRow(`SELECT key FROM KeyServer WHERE server = $1 and key = $2`,self_server_addr,f.Name()).Scan(&key);
                if key=="" {//如果文件里有，但数据库KeyServer没有这个服务器条目，就新增数据条目
                    log("新增数据块：",f.Name())
                    tx, err := db.Begin();checkErr(err)
                    _, err = tx.Exec(`INSERT INTO KeyServer VALUES ($1,$2);`,f.Name(),self_server_addr);checkErr(err)
                    err = tx.Commit();checkErr(err)
                }
                err = db.Close();checkErr(err)
            }*/
        }
    }

    if *enable_server {
        go tcpServer(*port)//启动服务器，接收客户端和其它服务器的消息
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
        //TODO:处理超时的连接
        //读取数据
        instruct := readInstruct(conn)
        if instruct==ERR {break}
        switch instruct {//根据指令码做出选择
            case DOWNLOAD_FILE://下载文件
                if global_server_load<253 {
                    global_server_load++
                    defer func(){global_server_load--}()
                }
                key:=readKey(conn)//读取文件key
                log("[接收到指令]客户端下载文件：",key)
                sendFile("storage/"+key,conn)//发送文件
                /*
                文件下载交互流程：
                客户端连接服务端
                客户端发送指令DOWNLOAD_FILE+文件key
                服务端发送文件大小（文件不存在则返回0）+文件内容
                客户端接收文件，直到接收到文件大小
                客户端关闭连接
                服务端关闭连接
                */
            case SYNC_DB://同步数据库
                log("[接收到指令]开始同步数据库")
                err:=reciveFile(DB_PATH,conn)
                if err!=nil {
                    fmt.Println("[ERROR]数据库同步出错")
                    break
                }
                acquireGlobalLock()
                decompressDatabase()
                releaseGlobalLock()
                sendInstruct(ACK,conn)
                fmt.Println("数据库同步完毕")
            case SYNC_SERVER_LIST://同步服务器列表
                log("[接收到指令]同步服务器列表")
                err:=reciveFile("server_list.txt",conn)
                if err!=nil {
                    fmt.Println("[ERROR]服务器列表同步出错")
                    break
                }
                sendInstruct(ACK,conn)
                refreshServerList()
                fmt.Println("服务器列表同步完毕")
            case UPLOAD_FILE:
                if global_server_load<253 {
                    global_server_load++
                    defer func(){global_server_load--}()
                }
                key := readKey(conn)//读取文件key
                log("[接收到指令]客户端上传文件：",key)
                err:=reciveFile("storage/"+key,conn)
                if err!=nil {
                    fmt.Println("[ERROR]客户端文件上传出错")
                    break
                }
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
                os.Remove("storage/"+key)
                sendInstruct(ACK,conn)
                /*
                文件删除交互流程：
                客户端连接服务端
                客户端发送指令DELETE_FILE+文件key
                服务端删除文件，返回ACK
                客户端关闭连接
                服务端关闭连接
                */
            case SEND_DB:
                log("[接收到指令]发送数据库")
                acquireGlobalLock()
                compressDatabase()
                releaseGlobalLock()
                sendFile(DB_PATH,conn)
                /*
                发送数据库交互流程：
                客户端连接服务端
                客户端发送指令SEND_DB
                服务端返回文件大小（8字节）+数据库
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
                log("对方IP：",server)
                time.Sleep(NET_TIMEOUT)//给时间给对方启动服务器
                test_conn, err := net.DialTimeout("tcp", server, NET_TIMEOUT)
                if err != nil {
                    fmt.Println("测试连接失败")
                    conn.Write([]byte{ERR})
                    continue //结束处理
                }
                fmt.Println("测试连接成功")
                test_conn.Close()
                //更新服务器列表
                file_datas, _ := ioutil.ReadFile("server_list.txt")
                if !strings.Contains(string(file_datas),server){
                    fmt.Println("新增服务器：",server)
                    file_server_list_strings:=strings.TrimSpace(string(file_datas))+"\r\n"+server
                    file_server_list, err := os.Create("server_list.txt");checkErr(err)
                    file_server_list.Write([]byte(file_server_list_strings))
                    file_server_list.Close()
                }else{
                    fmt.Println("服务器已在列表中：",server)
                }
                sendInstruct(ACK,conn)//返回ACK
                conn.Write([]byte(server))
                syncServerList()//向所有节点同步服务器列表
                refreshServerList()//刷新服务器列表
            case GET_SERVER_LIST:
                log("[接收到指令]请求服务器列表")
                sendFile("server_list.txt",conn)
                /*
                同步服务器列表交互流程：
                客户端连接服务端
                客户端发送指令GET_SERVER_LIST
                服务端发送文件大小（8字节）+服务器列表
                客户端关闭连接
                服务端关闭连接
                */
            case SERVER_LOAD:
                log("[接收到指令]查询服务器负载：",global_server_load)
                conn.Write([]byte{global_server_load})
            case ERR://中断连接
                break
        }
    }
}


func clientShell(){//客户端命令行
    fmt.Println(CLIENT_SHELL_WELCOME_MSG)
    for{
        fmt.Printf("GDUT-DSS:%s$ ",username)
        var command string
        var parameter [3] string
        fmt.Scanf("%s %s %s %s", &command, &parameter[0], &parameter[1], &parameter[2])
        switch command {
            case "help"://帮助
                fmt.Println(CLIENT_SHELL_HELP_MSG)
            case "exit"://退出
                os.Exit(0)
            case "login":
                if parameter[0]!=""{
                    username=parameter[0]
                    fmt.Println("用户登录：",username)
                }else{
                    fmt.Println("请输入用户名！")
                }
            case "get"://下载文件
                /*
                下载文件流程：
                先从文件数据库查询文件名对应的文件分块数量和校验码
                然后根据文件分块所在的服务器，智能选择每个分块的下载服务器
                所有文件分块下载完成后，合并成一个完整文件
                */
                updateServerList()
                //先从文件数据库读取文件名对应的key和服务器
                fmt.Println("查找数据库……")
                if parameter[1]=="" {
                    fmt.Println("请输入文件名！")
                    fmt.Println("用法：get [username] [filename]")
                    fmt.Println("例子：get yumi 1.7z")
                    continue
                }
                if !isPathExists(dbPath(parameter[0])) {
                    fmt.Println("数据库不存在！请检查命令或执行update命令更新。")
                    fmt.Println("用法：get [username] [filename]")
                    fmt.Println("例子：get yumi 1.7z")
                    continue
                }
                db, err := sql.Open(DB_TYPE, dbPath(parameter[0]));checkErr(err)//连接数据库
                //从数据库中读取文件名并新建下载任务
                var key_server_pair [] KeyServerPair
                //查询得到key_list
                var key_list [] string
                rows, err := db.Query(`SELECT key,num FROM FileKey WHERE filename=$1 ORDER BY num`,parameter[1]);checkErr(err)
                for rows.Next() {
                    var key string
                    var num int
                    if err = rows.Scan(&key,&num); err != nil {
                        rows.Close()
                        break
                    }
                    log(num,key)
                    key_list=append(key_list,key)
                }
                //查询每个key对应的服务器列表
                for n,key := range key_list{
                    key_server_pair=append(key_server_pair,KeyServerPair{Key:key})
                    rows, err := db.Query(`SELECT server FROM KeyServer WHERE key=$1 ORDER BY server`,key);checkErr(err)
                    for rows.Next() {
                        var server string
                        if err = rows.Scan(&server); err != nil {
                            rows.Close()
                            break
                        }
                        key_server_pair[n].Server=append(key_server_pair[n].Server,server)
                    }
                }
                err = db.Close();checkErr(err)
                //提交下载任务
                //难点：实现智能选择服务器，多线程下载
                //理想实现：看服务器带宽情况
                //实际实现：根据服务器连接数进行评分
                var best_server string
                for i,key_server := range key_server_pair{//每个key选择最佳服务器进行下载
                    //选择最佳服务器
                    best_server_load:=uint8(ERR)//服务器负载
                    for _,server := range key_server.Server{
                        server_load:=getServerLoad(server)
                        if server_load<=best_server_load {
                            best_server=server
                            best_server_load=server_load
                        }
                    }
                    if best_server_load==ERR {
                        fmt.Println("[ERROR]部分文件块所在服务器不在线，文件无法下载。")
                        os.Exit(1)
                    }
                    fmt.Println("提交下载任务",i,key_server.Key,best_server)
                    download_mission.Add()
                    go downloadFile(key_server.Key, best_server)
                }
                //等待下载完毕
                fmt.Println("等待下载完成……")
                download_mission.Wait()
                //合并文件
                fmt.Println("合并文件块……")
                file_full, _ := os.Create("download/"+parameter[1])
                for _,key := range key_list {
                    file_piece, err := os.Open("tmp/"+key);checkErr(err)
                    buf:=make([]byte, getFileSize("tmp/"+key))
                    file_piece.Read(buf)//全部读取
                    file_full.Write(buf)
                    file_piece.Close()
                    os.Remove("tmp/"+key)
                }
                file_full.Close()
                fmt.Println("文件下载成功")
            case "ls"://查看可下载的文件列表
                //直接从数据库中读取文件名并打印
                fmt.Println("")
                if parameter[0]=="-l" {
                    dir, err := ioutil.ReadDir("database");checkErr(err)
                    for _,f := range dir {
                        if(subString(f.Name(),0,1)=="."){continue}
                        db, err := sql.Open(DB_TYPE, "database/"+f.Name());checkErr(err)
                        rows, err := db.Query(`SELECT FileKey.filename,FileKey.num,FileKey.key,KeyServer.server FROM FileKey,KeyServer WHERE FileKey.key=KeyServer.key`);checkErr(err)
                        for rows.Next() {
                            var filename,key,server string
                            var num int
                            if err = rows.Scan(&filename,&num,&key,&server); err != nil {
                                rows.Close()
                                break
                            }
                            fmt.Println(filename,num,key,server)
                        }
                        err = db.Close();checkErr(err)
                    }
                    fmt.Println("")
                }else{
                    dir, err := ioutil.ReadDir("database");checkErr(err)
                    for _,f := range dir {
                        if(subString(f.Name(),0,1)=="."){continue}
                        fmt.Println(f.Name(),":")
                        db, err := sql.Open(DB_TYPE, "database/"+f.Name());checkErr(err)
                        rows, err := db.Query(`SELECT distinct(filename) FROM FileKey`);checkErr(err)
                        for rows.Next() {
                            var filename string
                            if err = rows.Scan(&filename); err != nil {
                                rows.Close()
                                break
                            }
                            fmt.Println("    ",filename)
                        }
                        fmt.Println("")
                        err = db.Close();checkErr(err)
                    }
                }
            case "put"://上传文件 TODO：多线程上传
                if username=="Anonymous" {
                    fmt.Println("请先登录！")
                    continue
                }
                /*
                上传文件流程：
                判断文件大小，如果超过分块数量，则切割成块
                计算所有分块的hash值并重命名
                选择服务器并上传文件块
                将文件信息写入数据库
                更新数据库到服务器
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
                    f, err := os.Open(file_path);checkErr(err)
                    buf_size:=FILE_BLOCK_SIZE
                    for i:=0;i<file_pieces_num;i++ {
                        if int(file_size)-FILE_BLOCK_SIZE*i<FILE_BLOCK_SIZE {
                            buf_size=int(file_size)-FILE_BLOCK_SIZE*i
                        }
                        buf:=make([]byte, buf_size)
                        f.ReadAt(buf,int64(FILE_BLOCK_SIZE*i))//每次读取一个FILE_BLOCK_SIZE
                        key:=hex.EncodeToString(hashBytes(buf))//计算key
                        fmt.Println("第",i,"个key：",key)
                        key_list=append(key_list,key)
                        //写入文件
                        file_piece, err := os.Create("tmp/"+key);checkErr(err)
                        file_piece.Write(buf)
                        file_piece.Close()
                    }
                    f.Close()
                    fmt.Println("文件分块完成！")
                }else{
                    fmt.Println("文件小于等于文件块大小，无需分块。")
                    f, err := os.Open(file_path);checkErr(err)
                    buf:=make([]byte, file_size)
                    f.Read(buf)//全部读取
                    key:=hex.EncodeToString(hashBytes(buf))//计算key
                    fmt.Println("第0个key：",key)
                    key_list=append(key_list,key)
                    //写入文件
                    file_piece, err := os.Create("tmp/"+key);checkErr(err)
                    file_piece.Write(buf)
                    file_piece.Close()
                    f.Close()
                }
                //选择服务器并上传文件块
                //查询数据库，计算每个服务器的文件数量，从小到大排序，排序相同的按服务器字符串排序
                //将一个分块发送到两个服务器上，然后重复上面的步骤，查询最佳服务器并继续上传
                fmt.Println("准备上传文件分块……")
                for i,key := range key_list {
                    //选择服务器 TODO：性能待优化
                    fmt.Println("查找数据库……")
                    if(!isPathExists(dbPath(username))){
                      fmt.Println("[INFO]没有数据库，新建中...")
                      db, err := sql.Open(DB_TYPE, dbPath(username));checkErr(err)
                      tx, err := db.Begin();checkErr(err)
                      _, err = tx.Exec(`CREATE TABLE KeyServer (
                      key string,
                      server string,
                      );`);checkErr(err)
                      _, err = tx.Exec(`CREATE TABLE FileKey (
                      filename string,
                      num int,
                      key string,
                      );`);checkErr(err)
                      err = tx.Commit();checkErr(err)
                      err = db.Close();checkErr(err)
                      fmt.Println("[INFO]数据库新建完成。")
                    }
                    dir, err := ioutil.ReadDir("database");checkErr(err)
                    servers := map[string]int{} //key为服务器ip，value为服务器上块的数量
                    for _,f := range dir {
                        if(subString(f.Name(),0,1)=="."){continue}
                        log(f.Name())
                        db, err := sql.Open(DB_TYPE, "database/"+f.Name());checkErr(err)
                        for _,server := range global_server_list {
                            var num int
                            err := db.QueryRow(`SELECT count(*) FROM KeyServer WHERE server=$1`,server).Scan(&num);checkErr(err)
                            servers[server]+=num
                            fmt.Println(server,num)
                        }
                        err = db.Close();checkErr(err)
                    }
                    //将server的key数量进行排序
                    servers_sorted:=sortMapByValue(servers)
                    fmt.Println("服务器及其块数量：",servers_sorted)
                    //fmt.Println("最佳上传服务器：",servers_sorted[0].Key,servers_sorted[0].Value)
                    //fmt.Println("次佳上传服务器：",servers_sorted[1].Key,servers_sorted[1].Value)
                    //连接服务器
                    server_upload:=make([]string,2)
                    server_upload_point:=0;
                    for{//上传第一个副本
                        conn, err := net.DialTimeout("tcp", servers_sorted[server_upload_point].Key, NET_TIMEOUT)
                        if err != nil {
                            fmt.Println("服务器连接失败：",servers_sorted[server_upload_point].Key)
                            if(server_upload_point>=servers_sorted.Len()){
                                fmt.Println("[ERROR]所有服务器连接失败，没有可上传的服务器！")
                                os.Exit(1)
                            }
                            server_upload_point++
                        }else{//如果服务器正常，就开始上传。TODO：两个副本同时上传（多线程）
                            fmt.Println("上传第",i,"个文件分块：",key,servers_sorted[server_upload_point].Key)
                            upload_mission.Add(1)
                            uploadFile(key,conn)
                            server_upload[0]=servers_sorted[server_upload_point].Key
                            server_upload_point++
                            break
                        }
                    }
                    for{//上传第二个副本
                        if(server_upload_point>=servers_sorted.Len()){
                            fmt.Println("[WARN]只连上一个服务器，该文件分块没有多副本！")
                            break
                        }
                        conn, err := net.DialTimeout("tcp", servers_sorted[server_upload_point].Key, NET_TIMEOUT)
                        if err != nil {
                            fmt.Println("服务器连接失败：",servers_sorted[server_upload_point].Key)
                            if(server_upload_point>=servers_sorted.Len()){
                                fmt.Println("[WARN]只连上一个服务器，该文件分块没有多副本！")
                                break
                            }
                            server_upload_point++
                        }else{//如果服务器正常，就开始上传。TODO：两个副本同时上传（多线程）
                            fmt.Println("上传第",i,"个文件分块：",key,servers_sorted[server_upload_point].Key)
                            upload_mission.Add(1)
                            uploadFile(key,conn)
                            server_upload[1]=servers_sorted[server_upload_point].Key
                            break
                        }
                    }
                    upload_mission.Wait()
                    //删除文件
                    err = os.Remove("tmp/"+key)
                    if err!=nil {
                        fmt.Println("[WARN]文件删除失败，可稍后手动删除。",err)
                    }
                    //写入数据库
                    fmt.Println("准备写入数据库……")
                    //写入数据库
                    db, err := sql.Open(DB_TYPE, dbPath(username));checkErr(err)//连接数据库
                    tx, err := db.Begin();checkErr(err)
                    _, err = tx.Exec(`INSERT INTO FileKey VALUES ($1,$2,$3);`,filename,i,key);checkErr(err)
                    _, err = tx.Exec(`INSERT INTO KeyServer VALUES ($1,$2);`,key,server_upload[0]);checkErr(err)
                    if(server_upload[1]!=""){
                        _, err = tx.Exec(`INSERT INTO KeyServer VALUES ($1,$2);`,key,server_upload[1]);checkErr(err)
                    }
                    err = tx.Commit();checkErr(err)
                    log("插入数据。",)
                    err = db.Close();checkErr(err)
                    fmt.Println("数据库更新成功。")
                }
                uploadDatabase()//同步数据库到其它服务器
                fmt.Println("文件上传完毕！")
            case "del"://删除文件
                if username=="Anonymous" {
                    fmt.Println("请先登录！")
                    continue
                }
                /*
                删除文件流程：
                将文件信息从数据库中删除
                更新数据库到服务器
                通知对应的服务器删除文件块
                */
                fmt.Println("准备写入数据库……")
                //查询key并删除
                db, err := sql.Open(DB_TYPE, dbPath(username));checkErr(err)//连接数据库
                rows, err := db.Query(`SELECT key FROM FileKey WHERE filename = $1`,parameter[0]);checkErr(err)
                var key_list [] string
                for rows.Next() {
                    var key string
                    if err = rows.Scan(&key); err != nil {
                        rows.Close()
                        break
                    }
                    key_list=append(key_list,key)
                }
                tx, err := db.Begin();checkErr(err)
                //先从KeyServer处删除key
                for _,key := range key_list {
                    _, err = tx.Exec(`DELETE FROM KeyServer WHERE key = $1`,key);checkErr(err)//删除文件
                }
                //然后从FileKey中删除file
                _, err = tx.Exec(`DELETE FROM FileKey WHERE filename = $1`,parameter[0]);checkErr(err)//删除文件
                err = tx.Commit();checkErr(err)
                err = db.Close();checkErr(err)
                fmt.Println("数据库更新成功。")
                //同步数据库到其它服务器
                uploadDatabase()
                //通知对应的服务器删除文件块 TODO:待优化，只通知存在的服务器删除
                log("通知服务器删除文件……")
                for _,key := range key_list {
                    bytes_buf := bytes.NewBuffer(make([]byte, 0))
                    binary.Write(bytes_buf, binary.BigEndian, DELETE_FILE)
                    binary.Write(bytes_buf, binary.BigEndian, []byte(key))
                    sendDatasToAllServers(bytes_buf.Bytes())
                }
                fmt.Println("文件删除完毕！")
            case "update":
                updateServerList()
                getGlobalDatabase()
            case "status":
                for _,server:= range global_server_list {
                    conn, err := net.DialTimeout("tcp", server, NET_TIMEOUT)
                    if err!=nil {
                        fmt.Println(server,"无法连接")
                    }
                    fmt.Println(server,"在线")
                    conn.Close()
                }
            case "debug"://调试
                switch parameter[0]{
                    case "1":

                }
        }
    }
}


func testConn(){//服务器加入集群时的测试连接函数
    //打开端口，测试连接
    tcpAddr, err := net.ResolveTCPAddr("tcp",":"+*port)
    tcpListener, err := net.ListenTCP("tcp",tcpAddr)
    if err != nil {
        fmt.Println("[ERROR]服务器启动错误：",err)
        panic("服务器启动错误")
    }
    fmt.Println("启动连接测试服务器！")
    //处理服务器测试连接
    tcpConn, _ := tcpListener.AcceptTCP()
    fmt.Println("接收到测试连接。")
    err=tcpConn.Close();checkErr(err)
    err=tcpListener.Close();checkErr(err)
    fmt.Println("服务器测试连接成功！")
}


/****************************************************/

//要对golang map按照value进行排序，思路是直接不用map，用struct存放key和value，实现sort接口，就可以调用sort.Sort进行排序了。
// A data structure to hold a key/value pair.
type Pair struct {
    Key   string
    Value int
}

// A slice of Pairs that implements sort.Interface to sort by Value.
type PairList []Pair

func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }

// A function to turn a map into a PairList, then sort and return it.
func sortMapByValue(m map[string]int) PairList {
    p := make(PairList, len(m))
    i := 0
    for k, v := range m {
        p[i] = Pair{k, v}
        i+=1;
    }
    sort.Sort(p)
    return p
}
