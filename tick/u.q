\d .u
/ Initialize subscriber tracking: create empty subscription list for each table
init:{w::t!(count t::tables`.)#()}
/ clreas subscriptions from .u.w

/ del:{w[x]_:w[x;;0]?y};.z.pc:{del[;x]each t};

/ Remove a subscriber from table's subscription list
/ Called when client disconnects (via .z.pc callback)
del:{[tab;handle]
    w[tab]_:w[tab;;0]?handle  / Find handle in subscriber list and remove it
  };
.z.pc:{del[;x]each t};  / Automatic cleanup: when client disconnects, remove from all tables

/ Filter table data by symbol subscription
/ If syms is backtick (all), return entire table; otherwise filter by subscription list
sel:{[tab;syms]
    $[`~syms;tab;select from tab where sym in syms]
    }



/ Publish data to all subscribers of a table
/ For each subscriber: filter data by their symbol subscription, then send
// pub:{[t;x]
//   {[t;x;w]
//     if[count x:sel[x]w 1];  / Filter data by subscriber's symbol filter
//     (neg first w)(`upd;t;x) / Send filtered data to subscriber (neg handle = send)
//   }[t;x]each w t             / Apply to each subscriber of table t
// }

/ Publish data to all subscribers
/ For each subscriber: filter data by their symbol subscription, then send
pub:{[tab;data]
  {[tab;data;w] 
     if[count data:sel[data]w 1;
     (neg first w)(`upd;tab;data)]
     }[tab;data]each w tab
  }

/ Add or update subscriber; return table schema for client initialization
/ If subscriber exists: update their symbol filter (union = add to existing)
/ If subscriber is new: add them to the list
/ Return: (table_name; initial_data_for_client)
// add:{$[(count w x)>i:w[x;;0]?.z.w;
//        .[`.u.w;(x;i;1);union;y];      / Subscriber exists: merge new symbols
//        w[x],:enlist(.z.w;y)];         / Subscriber new: add to list
//   (x;$[99=type v:value x;
//        sel[v]y;                       / Send filtered initial data
//        @[0#v;`sym;`g#]])}             / Or send empty schema with attribute

/ Add new subscriber or update existing one
add:{[tab;syms]
  $[(count w tab)>i:w[tab;;0]?.z.w;
      / Subscriber exists: merge new symbols with old ones
      .[`.u.w;(tab;i;1);union;syms];
      / Subscriber is new: add them to list
      w[tab],:enlist(.z.w;syms)
    ];
  / Return table name and initial data (or empty schema if table empty)
  (tab;$[99=type v:value tab;sel[v;syms];@[0#v;`sym;`g#]])
 }


/ Subscribe to table(s) with symbol filter
/ If table is backtick: subscribe to ALL tables with same filter
/ Otherwise: subscribe to one table; remove old subscription first, then add new
// sub:{if[x~`;:sub[;y]each t];           / If x is backtick, subscribe to all tables
//     if[not x in t;'x];                 / Validate table exists
//     del[x].z.w;                        / Remove any existing subscription
//     add[x;y]}                          / Add new subscription


sub: {[tab;syms]
  / If table is backtick, subscribe to all tables
  if[tab~`;:sub[;syms]each t];
  / Validate table exists
  if[not tab in t;'tab];
  / Remove any existing subscription
  del[tab].z.w;
  / Add new subscription
  add[tab;syms]
 }

/ Send end-of-day message to all connected subscribers
/ Collects all subscriber handles, negates them (for sending), then broadcasts .u.end[date]
// end:{(neg union/[w[;;0]])@\:(`.u.end;x)}

end:{[dt]
  (neg union/[w[;;0]])@\:(`.u.end;dt)
 }
