package main

import (
    "fmt"
    "strings"
    "net/http"
    "sync"
    "errors"
    "time"
    "math/rand"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

const (
    UserName = "root"
    Password = "Love19940710"
    IP = "127.0.0.1"
    Port = "3306"
    Database = "app"
)

const (
    LowPriority = 0
    MidPriority = 1
    HighPriority = 2
    TopPriority = 3
)

type IdGenerator struct {
    mutex sync.Mutex

    timestamp_bits uint
    worker_id_bits uint
    sequence_bits uint

    pre_timestamp int64
    worker_id int
    sequence int
}

type User struct {
    uid int
    username string
    nickname string
}

type Message struct {
    msg_id int64
    method string
    content string
    from_uid int
    to_uids []int
    priority int
    show bool
}

var db *sql.DB
var id_generator *IdGenerator
var user_info_list map[int]User
var user_online_list map[int]http.ResponseWriter
var user_online_message_list map[int][]Message
var user_offline_message_list map[int][]Message
var user_message_mutex map[int]sync.Mutex
var room_message_list map[int][]Message
var room_message_stat map[string]int
var room_message_list_mutex sync.RWMutex
var room_message_stat_mutex sync.Mutex
var user_fetch_bucket map[int]int
var user_fetch_offset map[int]int
var per_second_limit []int
var drop_probability []int
var message_priority []string

func (id_gen *IdGenerator) next_millisecond(pre int64) int64 {
    now := time.Now().UnixNano() / 1e6
    for now <= pre {
        now = time.Now().UnixNano() / 1e6
    }
    return now
}

func (id_gen *IdGenerator) get_id() int64 {
    now := time.Now().UnixNano() / 1e6
    id_gen.mutex.Lock()
    defer id_gen.mutex.Unlock()

    if now == id_gen.pre_timestamp {
        id_gen.sequence += 1
        if id_gen.sequence == (1 << id_gen.sequence_bits) {
            id_gen.sequence = 0
            now = id_gen.next_millisecond(now)
        }
    } else {
        id_gen.sequence = 0
    }
    id_gen.pre_timestamp = now

    var ret int64 = 0
    ret |= now << (id_gen.worker_id_bits+id_gen.sequence_bits)
    ret |= int64(id_gen.worker_id << id_gen.sequence_bits)
    ret |= int64(id_gen.sequence)
    return ret
}

func atoi(s string) int {
    ret := 0
    for _, i := range(s) {
        ret = ret * 10 + int(i - '0')
    }
    return ret
}

func raise_error(w http.ResponseWriter, content string) {
    err := errors.New(content)
    fmt.Fprintf(w, "%v\n", err)
}

func clear_message_stat() {
    for {
        time.Sleep(time.Second)
        now := time.Now().Unix()
        for i := LowPriority; i <= TopPriority; i++ {
            key := fmt.Sprintf("%d:%d", now-1, i)
            delete(room_message_stat, key)
        }
    }
}

func clear_room_message() {
    for {
        time.Sleep(time.Second * 10)
        second := time.Now().Second()
        if second < 5 {
            time.Sleep(time.Second)
        }

        bucket := get_bucket()
        bucket -= 20
        delete(room_message_list, bucket)
    }
}

func demotion(p int) bool {
    now := time.Now().Unix()
    key := fmt.Sprintf("%d:%d", now, p)
    room_message_stat_mutex.Lock()
    defer room_message_stat_mutex.Unlock()

    if(room_message_stat[key] > per_second_limit[p]) {
        val := rand.Intn(99)
        if val < drop_probability[p] {
            return true
        }
    }
    room_message_stat[key]++
    return false
}

func get_bucket() int {
    now := time.Now().Unix()
    return int(now / 10 * 10)
}

func fetch_message(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    form := r.Form
    if r.Method == "POST" {
        form = r.PostForm
    }

    if len(form["uid"]) == 0 || form["uid"][0] == "" {
        raise_error(w, "param error")
        return
    }

    uid := atoi(form["uid"][0])
    _, ok := user_online_list[uid]
    if !ok {
        raise_error(w, "you does not login")
        return
    }

    mutex, ok := user_message_mutex[uid]
    if !ok {
        mutex = sync.Mutex{}
        user_message_mutex[uid] = mutex
    }
    mutex.Lock()
    msg_list := user_online_message_list[uid]
    user_online_message_list[uid] = user_online_message_list[uid][:0]
    msg_list = append(msg_list, user_offline_message_list[uid]...)
    user_offline_message_list[uid] = user_offline_message_list[uid][:0]
    mutex.Unlock()

    pre_bucket, bucket := user_fetch_bucket[uid], get_bucket()
    offset, cnt := user_fetch_offset[uid], 10
    if bucket - pre_bucket > 10 {
        pre_bucket = bucket - 10
        offset = 0
    }

    room_message_list_mutex.RLock()
    if pre_bucket != bucket && offset < len(room_message_list[pre_bucket]) {
        cnt -= len(room_message_list[pre_bucket]) - offset
        msg_list = append(msg_list, room_message_list[pre_bucket][offset:]...)
        user_fetch_offset[uid] = 0
    }
    if cnt > 0 {
        if pre_bucket != bucket {
            offset = 0
        }
        if offset+cnt > len(room_message_list[bucket]) {
            cnt = len(room_message_list[bucket]) - offset
        }
        if cnt != 0 {
            msg_list = append(msg_list, room_message_list[bucket][offset:offset+cnt]...)
        }
        user_fetch_offset[uid] = offset+cnt
    }
    user_fetch_bucket[uid] = bucket
    room_message_list_mutex.RUnlock()

    resp := ""
    for _, msg := range(msg_list) {
        if msg.show{
            if strings.HasPrefix(msg.method, "room") && len(msg.to_uids) != 0 {
                var id int
                for _, id = range(msg.to_uids) {
                    if uid == id {
                        break
                    }
                }
                if uid != id {
                    continue
                }
            }
            user := user_info_list[msg.from_uid]
            resp = fmt.Sprintf("%smsg_id : %d\n", resp, msg.msg_id)
            resp = fmt.Sprintf("%sfrom : %s\n", resp, user.username)
            resp = fmt.Sprintf("%smethod : %s\n", resp, msg.method)
            resp = fmt.Sprintf("%scontent : %s\n", resp, msg.content)
            resp = fmt.Sprintf("%spriority : %s\n", resp, message_priority[msg.priority])
            resp = fmt.Sprintf("%sshow : %v\n\n", resp, msg.show)
        }
    }
    fmt.Fprintf(w, "%s", resp)
}

func push_message(from int, to []int, method, content string, show bool, p int) {
    msg := Message{
        msg_id : id_generator.get_id(),
        method : method,
        from_uid : from,
        to_uids : to,
        content : content,
        priority : p,
        show : show,
    }

    if strings.HasPrefix(method, "room") {
        if !demotion(p) {
            bucket := get_bucket()
            room_message_list_mutex.Lock()
            defer room_message_list_mutex.Unlock()

            room_message_list[bucket] = append(room_message_list[bucket], msg)
        }
        return
    }

    to_uid := to[0]
    mutex, ok := user_message_mutex[to_uid]
    if !ok {
        mutex = sync.Mutex{}
        user_message_mutex[to_uid] = mutex
    }
    mutex.Lock()
    defer mutex.Unlock()

    temp := user_online_message_list
    _, ok = user_online_list[to_uid]
    if !ok {
        temp = user_offline_message_list
    }
    temp[to_uid] = append(temp[to_uid], msg)
}

func room_chat(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    form := r.Form
    if r.Method == "POST" {
        form = r.PostForm
    }

    if len(form["uid"]) == 0 || form["uid"][0] == "" || len(form["content"]) == 0 {
        raise_error(w, "param error")
        return
    }

    uid := atoi(form["uid"][0])
    if _, ok := user_online_list[uid]; !ok {
        raise_error(w, "you does not login")
        return
    }

    if form["content"][0] == "" {
        raise_error(w, "content cant be null")
        return
    }

    content := form["content"][0]
    to_uids := []int{}
    push_message(uid, to_uids, "room_chat", content, true, MidPriority)
}

func chat(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    form := r.Form
    if r.Method == "POST" {
        form = r.PostForm
    }

    if len(form["uid"]) == 0 || form["uid"][0] == "" || len(form["to_uid"]) ==0 || form["to_uid"][0] == "" || len(form["content"]) == 0 {
        raise_error(w, "param error")
        return
    }

    uid := atoi(form["uid"][0])
    if _, ok := user_online_list[uid]; !ok {
        raise_error(w, "you does not login")
        return
    }

    if form["content"][0] == "" {
        raise_error(w, "content cant be null")
        return
    }

    to_uid := atoi(form["to_uid"][0])
    content := form["content"][0]
    to_uids := []int{to_uid}
    push_message(uid, to_uids, "chat", content, true, TopPriority)
}

func update_online_status(uid int, status bool, w http.ResponseWriter) {
    pre_w, ok := user_online_list[uid]
    if ok && status {
        fmt.Fprintf(pre_w, "you login elsewhere\n")
    }
    if status {
        fmt.Fprintf(w, "login success\n")
        user_online_list[uid] = w
    } else {
        delete(user_online_list, uid)
        fmt.Fprintf(w, "logout success\n")
    }
}

func register(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    form := r.Form
    if r.Method == "POST" {
        form = r.PostForm
    }

    if len(form["username"]) == 0 || form["username"][0] == "" {
        raise_error(w, "username cant be null")
        return
    }

    if len(form["password"]) == 0 || form["password"][0] == "" {
        raise_error(w, "password cant be null")
        return
    }

    if len(form["confirm_password"]) == 0 || form["confirm_password"][0] == "" {
        raise_error(w, "please confirm password")
        return
    }

    username := form["username"][0]
    password := form["password"][0]
    confirm_password := form["confirm_password"][0]

    if password != confirm_password {
        raise_error(w, "password is not same")
        return
    }

    rows, err := db.Query("select username from user where username=?", username)
    if err != nil {
        panic(err)
    }
    name := ""
    for rows.Next() {
        err = rows.Scan(&name)
        if err != nil {
            panic(err)
        }
    }

    if name != "" {
        raise_error(w, "username is existed")
        return
    }

    stmt, _ := db.Prepare("insert into user(username, password) values(?,?)")
    stmt.Exec(username, password)
    rows, _ = db.Query("select uid from user where username=?", username)
    var uid int
    for rows.Next() {
        rows.Scan(&uid)
    }
    nickname := fmt.Sprintf("user_%d", uid)
    stmt, _ = db.Prepare("update user set nickname=? where uid=?")
    stmt.Exec(nickname, uid)
    stmt.Close()
    fmt.Fprintf(w, "register success\n")
}

func login(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    form := r.Form
    if r.Method == "POST" {
        form = r.PostForm
    }

    if len(form["username"]) == 0 || form["username"][0] == "" {
        raise_error(w, "username cant be null")
        return
    }
    if len(form["password"]) == 0 || form["password"][0] == "" {
        raise_error(w, "password cant be null")
        return
    }

    username := form["username"][0]
    password := form["password"][0]

    var user User
    var pw string
    var uid int
    rows, err := db.Query("select uid, username, password, nickname from user where username=?", username)
    if err != nil {
        panic(err)
    }
    for rows.Next() {
        rows.Scan(&uid, &user.username, &pw, &user.nickname)
        user.uid = uid
        user_info_list[uid] = user
    }

    if user.username == "" {
        raise_error(w, "user is not existed")
        return
    }
    if pw != password {
        raise_error(w, "password is false")
        return
    }
    update_online_status(uid, true, w)
    content := fmt.Sprintf("%s login", user.username)
    to_uids := []int{}
    push_message(uid, to_uids, "room_enter", content, true, MidPriority)
}

func logout(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()
    form := r.Form
    if r.Method == "POST" {
        form = r.PostForm
    }

    if len(form["uid"]) == 0 || form["uid"][0] == "" {
        raise_error(w, "param error")
        return
    }
    uid := atoi(form["uid"][0])
    _, ok := user_online_list[uid]
    if !ok {
        raise_error(w, "user does not login")
        return
    }
    update_online_status(uid, false, w)
    content := fmt.Sprintf("%s logout", user_info_list[uid].username)
    to_uids := []int{}
    push_message(uid, to_uids, "room_leave", content, true, MidPriority)
}

func Hello(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "welcome\n")
}

func init_id_generator() {
    id_generator = &IdGenerator{}

    id_generator.mutex = sync.Mutex{}
    id_generator.timestamp_bits = 41
    id_generator.worker_id_bits = 10
    id_generator.sequence_bits = 12

    id_generator.pre_timestamp = 0
    id_generator.worker_id = 0
    id_generator.sequence = 0
}

func init() {
    init_id_generator()
    user_info_list = make(map[int]User)
    user_online_list = make(map[int]http.ResponseWriter)
    user_online_message_list = make(map[int][]Message)
    user_offline_message_list = make(map[int][]Message)
    user_message_mutex = make(map[int]sync.Mutex)
    room_message_list = make(map[int][]Message)
    room_message_stat = make(map[string]int)
    room_message_list_mutex = sync.RWMutex{}
    room_message_stat_mutex = sync.Mutex{}
    user_fetch_bucket = make(map[int]int)
    user_fetch_offset = make(map[int]int)
    per_second_limit = []int{3, 4, 5, 6}
    drop_probability = []int{100, 50, 30, 0}
    message_priority = []string{"low", "mid", "high", "top"}

    http.HandleFunc("/register", register)
    http.HandleFunc("/login", login)
    http.HandleFunc("/logout", logout)
    http.HandleFunc("/fetch_message", fetch_message)
    http.HandleFunc("/chat", chat)
    http.HandleFunc("/room/chat", room_chat)
    http.HandleFunc("/", Hello)
}

func main() {
    cnt := 0
    now := time.Now().UnixNano()
    end_time := now + 1e9
    for now == end_time {
        id_generator.get_id()
        cnt++
        now = time.Now().UnixNano()
    }
    fmt.Printf("%d\n", cnt)

    path := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", UserName, Password, IP, Port, Database)
    conn, err := sql.Open("mysql", path)
    if err != nil {
        panic(err)
    }
    db = conn
    defer db.Close()

    go clear_message_stat() 
    go clear_room_message()

    err = http.ListenAndServe("", nil)
    if err != nil {
        panic(err)
    }
}
