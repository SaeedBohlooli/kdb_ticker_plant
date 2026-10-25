/ q tick/feed.q
/ dummy feed: publishes random trades and quotes to the tickerplant on :5010

/ async handle to the tickerplant (negative handle = fire-and-forget)
h:neg hopen `::5010

.feed.syms:`AAPL`MSFT`AMZN`GOOGL`TSLA`META
/ prices must be floats (9h) to match the float price/bid/ask columns in sym.q
.feed.price:.feed.syms!131 247 105 2144 648 163f

/ returns a multiplier slightly below (-) or above (+) 1, e.g. 0.98 or 1.03
priceMultiplier:{[plusMinus]
    plusMinus[1;rand 0.05]
    }

getBid:{[syms]
    priceMultiplier[-] * .feed.price[syms]
    }

getAsk:{[syms]
    priceMultiplier[+] * .feed.price[syms]
    }

/ random walk: move every price up or down by up to 0.5%
updatePrices:{[]
    .feed.price:{priceMultiplier[rand (+;-)] * x}'[.feed.price]
    }

.z.ts:{
    updatePrices[];
    n:rand 6;
    syms:n?.feed.syms;
    / columns must be in the same order as the schema in sym.q
    $[rand 2;
        h(`.u.upd;`trade;(n#.z.N;syms;.feed.price[syms];n? rand 100i));
        h(`.u.upd;`quote;(n#.z.N;syms;getBid'[syms];getAsk'[syms];n?rand 100;n?rand 100))
     ]
    }

\t 100
