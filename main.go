package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type Message struct {
	TimeLocal                    time.Time
	BytesSent                    int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

type Reader interface {
	Read(rc chan []byte)
}

type Writer interface {
	Write(wc chan *Message)
}

type LogProcess struct {
	rc     chan []byte
	wc     chan *Message
	reader Reader
	writer Writer
}

type ReadFromFile struct {
	path string // file path
}
type WriteToInfluxDB struct {
	influxDns string // influxdb data source
}

type SystemMonitor struct {
	HandleLine   int     `json:"handleLine"`
	Tps          float64 `json:"tps"`
	ReadChanLen  int     `json:"readChanLen"`
	WriteChanLen int     `json:"writeChanLen"`
	RunTime      string  `json:"runTime"`
	ErrNum       int     `json:"errNum"`
}

type Monitor struct {
	startTime time.Time
	data      SystemMonitor
	tpsSli    []int
}

const TypeHandleLine = 0
const TypeErrNum = 1

var TypeMonitorChan = make(chan int, 200)
var re = regexp.MustCompile(`[][" ]+`)

func (m *Monitor) start(lp *LogProcess) {
	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeHandleLine:
				m.data.HandleLine += 1
			case TypeErrNum:
				m.data.ErrNum += 1

			}
		}
	}()

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			m.tpsSli = append(m.tpsSli, m.data.HandleLine)
			if len(m.tpsSli) > 2 {
				m.tpsSli = m.tpsSli[1:]
			}
		}
	}()

	http.HandleFunc("/monitor", func(w http.ResponseWriter, r *http.Request) {
		m.data.RunTime = time.Now().Sub(m.startTime).String()
		m.data.ReadChanLen = len(lp.rc)
		m.data.WriteChanLen = len(lp.wc)

		if len(m.tpsSli) >= 2 {
			m.data.Tps = float64(m.tpsSli[1]-m.tpsSli[0]) / 5
		}

		bytes, err := json.MarshalIndent(m.data, "", "\t")
		if err != nil {
			log.Println(err)
		}
		io.WriteString(w, string(bytes))
	})

	http.ListenAndServe(":8888", nil)
}

func (r *ReadFromFile) Read(rc chan []byte) {
	// Open file
	file, err := os.Open(r.path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Read one line by line
	file.Seek(0, 2)
	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadBytes('\n')

		if err == io.EOF {
			time.Sleep(time.Millisecond * 1000)
			continue
		} else if err != nil {
			panic(err)
		}
		TypeMonitorChan <- TypeHandleLine
		rc <- line[:len(line)-1]
	}

}

func (w *WriteToInfluxDB) Write(wc chan *Message) {
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
		//Username: username,
		//Password: password,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	for v := range wc {
		// Create a new point batch
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  "mydb",
			Precision: "s",
		})
		if err != nil {
			log.Fatal(err)
		}

		// Create a point and add to batch
		tags := map[string]string{"Path": v.Path, "Method": v.Method,
			"Schema": v.Scheme, "Status": v.Status}
		fields := map[string]interface{}{
			"UpstreamTime": v.UpstreamTime,
			"RequestTime":  v.RequestTime,
			"BytesSent":    v.BytesSent,
		}

		pt, err := client.NewPoint("nginx_log", tags, fields, time.Now())
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)

		// Write the batch
		if err := c.Write(bp); err != nil {
			log.Fatal(err)
		}
	}
}

func (l *LogProcess) Process() {
	// 解析模块
	loc, _ := time.LoadLocation("Asia/Shanghai")

	for v := range l.rc {
		matches := re.Split(string(v), -1)
		bytesSent, _ := strconv.Atoi(matches[10])
		parse, err := time.ParseInLocation("2/Jan/2006:15:04:05 +0000", matches[3]+" "+matches[4], loc)
		if err != nil {
			panic(err)
		}
		upsteamTime, _ := strconv.ParseFloat(matches[15], 64)

		msg := &Message{
			TimeLocal:    parse,
			BytesSent:    bytesSent,
			Path:         matches[7],
			Method:       matches[6],
			Scheme:       matches[5],
			Status:       matches[9],
			UpstreamTime: upsteamTime,
			RequestTime:  upsteamTime,
		}
		l.wc <- msg
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	r := &ReadFromFile{
		path: "testdata/access.log",
	}
	w := &WriteToInfluxDB{
		influxDns: "test@test",
	}

	lp := &LogProcess{
		rc:     make(chan []byte, 200),
		wc:     make(chan *Message, 200),
		reader: r,
		writer: w,
	}

	for i := 0; i < 2; i++ {
		go lp.reader.Read(lp.rc)
		go lp.Process()
	}

	for i := 0; i < 4; i++ {
		go lp.writer.Write(lp.wc)
	}

	m := &Monitor{
		startTime: time.Now(),
		data:      SystemMonitor{},
	}

	m.start(lp)
}
