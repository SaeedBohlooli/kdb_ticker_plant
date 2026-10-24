
"kdb+tick 2.8 2014.03.12"

/q tick.q SRC [DST] [-p 5010] [-o h]
system"l tick/",(src:first .z.x,enlist"sym"),".q"

if[not system"p";system"p 5010"]

\l tick/u.q
\d .u
// ld:{if[not type key L::`$(-10_string L),string x;.[L;();:;()]];i::j::-11!(-2;L);if[0<=type i;-2 (string L)," is a corrupt log. Truncate to length ",(string last i)," and restart";exit 1];hopen L};

ld:{[dt]
    if [not type key L::`$(-10_string L),string dt;
        .[L;();:;()]
    ];
    i:j::-11!(-2;L);
    if[0<=type i;
        -2 (string L)," is a corrupt log. Truncate to length ",(string last i)," and restart";
        exit 1
    ];
    hopen L;
    
 }

tick:{[src;tplog]
    init[];
    / check for 'time 'sym cols
    if[not min(`time`sym~2#key flip value@)each t;
        '`timesym
    ];
    / apply grouped attribute
    @[;`sym;`g#]each t;
    d::.z.D;
    / create handle to tplog
    if[l::count tplog;
        L::`$":",tplog,"/",src,10#".";
        l::ld d
    ]
};

// tick:{init[];if[not min(`time`sym~2#key flip value@)each t;'`timesym];@[;`sym;`g#]each t;d::.z.D;if[l::count y;L::`$":",y,"/",x,10#".";l::ld d]};

// endofday:{end d;d+:1;if[l;hclose l;l::0(`.u.ld;d)]};
endofday:{end d;d+:1;if[l;hclose l;l::0(`.u.ld;d)]};

// ts:{if[d<x;if[d<x-1;system"t 0";'"more than one day?"];endofday[]]};

ts:{[dt]
    if[d<dt;
      if[d<dt-1; system"t 0";'"more than one day?"];
      endofday[]
    ]};


/ BATCH MODE: Timer is enabled, use batch publishing
/ Publishes data once per timer tick (1000ms) instead of immediately
if[system"t";
  
  / Timer callback: Executed every timer tick (1000ms by default)
  .z.ts:{
    / Publish current values of all tables in batch
    / pub' = call pub for each table in parallel
    / value each t = get current data from each table
    pub'[t;value each t];
    
    / Reapply grouping attribute to symbol column for performance
    / @[`.;t;...] = modify global namespace for all tables
    / After publishing, tables are cleared, need to restore g# on empty tables
    @[`.;t;@[;`sym;`g#]0#];
    
    / Reset message counters
    / i = messages in log file
    / j = total messages (log + buffer)
    / After batch publish, both are equal again
    i::j;
    
    / Check if date has changed (detect end-of-day)
    ts .z.D;
  };
  
  / Data insertion function: Handles incoming trades/quotes
  upd:{[tab;data]
    / Handle timestamp: Add time if not already present
    if[not -16=type first first data;
      / Check if date changed (day boundary detection)
      if[d<"d"$a:.z.P;
          .z.ts[]
          ];
      / Convert time to nanosecond number
      a:"n"$a;
      / Prepend timestamp to data:
      / If single row: a,data
      / If multiple rows: (enlist(count first data)#a),data
      data:$[0>type first data;
           a,data;
           (enlist(count first data)#a),data]
    ];
    
    / Insert data into table
    tab insert data;
    
    / Log to disk (if logging enabled)
    if[l;  l enlist (`upd;tab;data);  j+:1  ];
      / Write to log file: (`upd; table_name; data)
     
      / Increment total message counter
     
   
  }
];

// if[not system"t";system"t 1000";
//  .z.ts:{ts .z.D};
//  upd:{[t;x]ts"d"$a:.z.P;
//  if[not -16=type first first x;a:"n"$a;x:$[0>type first x;a,x;(enlist(count first x)#a),x]];
//  f:key flip value t;pub[t;$[0>type first x;enlist f!x;flip f!x]];if[l;l enlist (`upd;t;x);i+:1];}];



if[not system"t";system"t 1000";
 .z.ts:{ts .z.D};
 upd:{[t;x]ts"d"$a:.z.P;
 if[not -16=type first first x;a:"n"$a;x:$[0>type first x;a,x;(enlist(count first x)#a),x]];
 f:key flip value t;pub[t;$[0>type first x;enlist f!x;flip f!x]];if[l;l enlist (`upd;t;x);i+:1];}];

\d .
.u.tick[src;.z.x 1];

\
 globals used
 .u.w - dictionary of tables->(handle;syms)
 .u.i - msg count in log file
 .u.j - total msg count (log file plus those held in buffer)
 .u.t - table names
 .u.L - tp log filename, e.g. `:./sym2008.09.11
 .u.l - handle to tp log file
 .u.d - date

/test
>q tick.q
>q tick/ssl.q

/run
>q tick.q sym  .  -p 5010	/tick
>q tick/r.q :5010 -p 5011	/rdb
>q sym            -p 5012	/hdb
>q tick/ssl.q sym :5010		/feed
