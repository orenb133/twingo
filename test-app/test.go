package main

import (
    "fmt"
    "time"
    "encoding/json"
    "github.com/op/go-logging"
    "github.com/orenb133/twingo"
)

func main() {



    ch1 := make(chan *twingo.MarketBar, 1)
    logging.SetFormatter(logging.GlogFormatter)
	logging.SetLevel(logging.INFO, "")
    listener := twingo.NewMarketDataListener(logging.MustGetLogger("Listener"))
    dispatcher := twingo.NewMarketDataEventsDispatcher(logging.MustGetLogger("Dispatcher"))
    aggregator := twingo.NewMarketBarsAggregator(logging.MustGetLogger("Aggregator"))
    
   
    listener.InitBaseUrl("wss://api.gemini.com/v1/marketdata")
    listener.InitSymbol(twingo.MarketDataSymbolBtcUsd)
    listener.InitEgressChannel(dispatcher.GetIngressChannel())
    dispatcher.InitRegisterEgressTradeChannel(aggregator.GetIngressChannel())
    aggregator.InitBarDuration(5 * time.Minute)
    aggregator.InitEgressChannel(ch1)
    
    err := listener.Connect()
    
    if err != nil {
    	fmt.Println(err)
    	
    	return
    }
    
    go listener.Listen()
	go dispatcher.Dispatch()
	go aggregator.Aggregate()
	
    for bar := range ch1 {
    	v, _ := json.Marshal(bar)
		fmt.Println(string(v))
    }
}