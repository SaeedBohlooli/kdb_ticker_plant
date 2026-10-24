h:neg hopen `::5010

.feed.syms: `AAPL`MFXt`AMZN`GOOGL`TSLA`META
.feed.price: .feed.syms!131 247 105 2144 648 163

priceMultiplier:{[plusMinus]
    plusMinus[1; rand 0.05]
    }

getBids:{[syms]
    priceMultiplier[-] * .feed.price[syms]
    }

getAsks:{[syms]
    priceMultiplier[+] * .feed.price[syms]
    }

updatePrices:{[syms]
    // .feed.price[syms]: (getBids[syms] + getAsks[syms]) % 2
    .feed.price: priceMultiplier[rand (+;-)] * x}'[feed.prices]
    }
.z.ts:{
    updatePrices[];
    n:rand 6;
    syms:n?.feed.syms;

    $[rand 2;
        h(".u.upd";`trade;(n#.z.N;syms;.feed.price[syms];n?rand 100));  
        h(".u.upd";`quote;getBid'[syms];getAsk'[syms];n?rand 100;n?rand 100))
    ]
    }
\t 100