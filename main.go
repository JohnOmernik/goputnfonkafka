package main

import (
    "flag"
    "fmt"
    "net"
    "os"
    "time"
    "log"
    "strings"
    "strconv"
    "encoding/json"
    "github.com/Shopify/sarama"
    "github.com/fln/nf9packet"
    "github.com/samuel/go-zookeeper/zk"
)
var dumpJSON bool
var pretty bool

var brokers string
var verbose bool

var myprod sarama.AsyncProducer
/*var certFile string
var keyFile string
var caFile string
var verifySsl bool
*/
var zks string
var zknode string

type templateCache map[string]*nf9packet.TemplateRecord


func prodRecords(template *nf9packet.TemplateRecord, records []nf9packet.FlowDataRecord) {

    for _, r := range records {
        recs := make(map[string]string)
        for i := range r.Values {
            recs[template.Fields[i].Name()] = template.Fields[i].DataToString(r.Values[i])
        }
        json, _ := json.Marshal(recs)

        msg := &sarama.ProducerMessage{
                Topic: "netflow",
                Key: nil,
                Value: sarama.StringEncoder(json)}
        myprod.Input() <- msg


        if verbose{
            fmt.Printf("%s\n", json)
        }
    }
}





func packetDump(addr string, data []byte, cache templateCache) {
    p, err := nf9packet.Decode(data)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        return
    }

    templateList := p.TemplateRecords()
    flowSets := p.DataFlowSets()

    for _, t := range templateList {
        templateKey := fmt.Sprintf("%s|%b|%v", addr, p.SourceId, t.TemplateId)
        cache[templateKey] = t
    }

    for _, set := range flowSets {
        templateKey := fmt.Sprintf("%s|%b|%v", addr, p.SourceId, set.Id)
        template, ok := cache[templateKey]
        if !ok {
            // We do not have template for this Data FlowSet yet
            continue
        }

        records := template.DecodeFlowSet(&set)
        if records == nil {
            // Error in decoding Data FlowSet
            continue
        }
        prodRecords(template, records)
    }
}

func newProducer(brokerList []string) sarama.AsyncProducer {

    // For the access log, we are looking for AP semantics, with high throughput.
    // By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
    config := sarama.NewConfig()
 //   tlsConfig := createTlsConfiguration()
//    if tlsConfig != nil {
 //       config.Net.TLS.Enable = true
   //     config.Net.TLS.Config = tlsConfig
    //}
    config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
    config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
    config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

    producer, err := sarama.NewAsyncProducer(brokerList, config)
    if err != nil {
        log.Fatalln("Failed to start Sarama producer:", err)
    }

    // We will just log to STDOUT if we're not able to produce messages.
    // Note: messages will only be returned here after all retry attempts are exhausted.
    go func() {
        for err := range producer.Errors() {
            log.Println("Failed to write Netflow entry:", err)
        }
    }()

    return producer
}




func getbrokers(zks string, zknode string) string{

    var arzks[] string
    arzks = strings.Split(zks, ",")
    c, _, err := zk.Connect(arzks, time.Second)
    if err != nil {
        panic(err)
    }
    var node string
    node  = "/" + zknode + "/brokers/ids"
    children, _, err := c.Children(node)
    if err != nil {
        panic(err)
    }
    var f interface{}
    var outbrokers string = ""

    for _, v := range children {
        var cnode string = node + "/" + v
        n, _, _ := c.Get(cnode)
        err := json.Unmarshal(n, &f)
        if err != nil {
            panic(err)
        }
        m := f.(map[string]interface{})
        var myhost string = m["host"].(string)
        var myport float64 = m["port"].(float64)
        sport := strconv.FormatFloat(myport, 'f', -1, 64)
        if outbrokers != "" {
            outbrokers = outbrokers + "," + myhost + ":" + sport
        } else {
            outbrokers = myhost + ":" + sport
        }
    }

    return outbrokers

}


func main() {
    listenAddr := flag.String("listen", ":9995", "Address to listen for NetFlow v9 packets.")

    flag.BoolVar(&dumpJSON, "json", false, "Dump packet in JSON instead of plain text.")
    flag.BoolVar(&pretty, "pretty", false, "When used with -json dump json in a pretty way.")
    flag.BoolVar(&verbose, "verbose", false, "Turn on Sarama logging")

 /*
    flag.BoolVar(&verifySsl,  "verify", false, "Optional verify ssl certificates chain")
    certFile := flag.String("certificate", "", "The optional certificate file for client authentication")
    keyFile := flag.String("key", "", "The optional key file for client authentication")
    caFile := flag.String("ca", "", "The optional certificate authority file for TLS client authentication")
*/
    brokers := flag.String("brokers", "", "The Kafka brokers to connect to, as a comma separated list")
    zks := flag.String("zks", "", "Either brokers or zks must be provided")
    zknode := flag.String("zknode", "", "Root of zk ")
    flag.Parse()

    if *brokers == "" && *zks != "" {
        *brokers = getbrokers(*zks, *zknode)
        fmt.Printf("Hey I got your brokers from ZK!: %s\n", *brokers)
    }



    if verbose {
        sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
    }

    if *brokers == "" {
        flag.PrintDefaults()
        os.Exit(1)
    }

    brokerList := strings.Split(*brokers, ",")
    log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))


    fmt.Printf("Opening Port\n")
    addr, err := net.ResolveUDPAddr("udp", *listenAddr)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Listening\n")
    con, err := net.ListenUDP("udp", addr)
    if err != nil {
        panic(err)
    }

    data := make([]byte, 8960)
    cache := make(templateCache)
    myprod = newProducer(brokerList)

    for {
        length, remote, err := con.ReadFrom(data)
        if err != nil {
            panic(err)
        }

        packetDump(remote.String(), data[:length], cache)
    }
}
