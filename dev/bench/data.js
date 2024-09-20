window.BENCHMARK_DATA = {
  "lastUpdate": 1726846876359,
  "repoUrl": "https://github.com/spiraldb/vortex",
  "entries": {
    "Vortex bytes_at": [
      {
        "commit": {
          "author": {
            "email": "dan@spiraldb.com",
            "name": "Dan King",
            "username": "danking"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "80c23d50b6a3730eb606953ad72ad0650312bd89",
          "message": "fix: version -> benchmark in GHA bench.yml (#893)\n\nI renamed this in bench-pr.yml and changed the references to the matrix\r\nvariable in both files, but forgot to rename it here in bench.yml",
          "timestamp": "2024-09-20T11:19:02-04:00",
          "tree_id": "6af96dd7d72f3764f2a6959809024555610ae461",
          "url": "https://github.com/spiraldb/vortex/commit/80c23d50b6a3730eb606953ad72ad0650312bd89"
        },
        "date": 1726845950243,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "bytes_at/array_data",
            "value": 630.0797154495973,
            "unit": "ns",
            "range": 0.4040898979544636
          },
          {
            "name": "bytes_at/array_data #2",
            "value": 1036.9092623934907,
            "unit": "ns",
            "range": 0.6229287397039798
          }
        ]
      }
    ],
    "Vortex random_access": [
      {
        "commit": {
          "author": {
            "email": "dan@spiraldb.com",
            "name": "Dan King",
            "username": "danking"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "80c23d50b6a3730eb606953ad72ad0650312bd89",
          "message": "fix: version -> benchmark in GHA bench.yml (#893)\n\nI renamed this in bench-pr.yml and changed the references to the matrix\r\nvariable in both files, but forgot to rename it here in bench.yml",
          "timestamp": "2024-09-20T11:19:02-04:00",
          "tree_id": "6af96dd7d72f3764f2a6959809024555610ae461",
          "url": "https://github.com/spiraldb/vortex/commit/80c23d50b6a3730eb606953ad72ad0650312bd89"
        },
        "date": 1726845972545,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "vortex/tokio local disk",
            "value": 1265918.2491115367,
            "unit": "ns",
            "range": 5106.1060879332945
          },
          {
            "name": "vortex/localfs",
            "value": 1455274.151722514,
            "unit": "ns",
            "range": 27647.030753519153
          },
          {
            "name": "parquet/tokio local disk",
            "value": 194984908.03333333,
            "unit": "ns",
            "range": 3574661.382499993
          }
        ]
      }
    ],
    "Vortex DataFusion": [
      {
        "commit": {
          "author": {
            "email": "dan@spiraldb.com",
            "name": "Dan King",
            "username": "danking"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "80c23d50b6a3730eb606953ad72ad0650312bd89",
          "message": "fix: version -> benchmark in GHA bench.yml (#893)\n\nI renamed this in bench-pr.yml and changed the references to the matrix\r\nvariable in both files, but forgot to rename it here in bench.yml",
          "timestamp": "2024-09-20T11:19:02-04:00",
          "tree_id": "6af96dd7d72f3764f2a6959809024555610ae461",
          "url": "https://github.com/spiraldb/vortex/commit/80c23d50b6a3730eb606953ad72ad0650312bd89"
        },
        "date": 1726846071312,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "arrow/planning",
            "value": 821684.2998366854,
            "unit": "ns",
            "range": 2541.9077616096474
          },
          {
            "name": "arrow/exec",
            "value": 1769622.7571484672,
            "unit": "ns",
            "range": 1364.3998261914821
          },
          {
            "name": "vortex-pushdown-compressed/planning",
            "value": 515451.0811567107,
            "unit": "ns",
            "range": 1088.5329503435642
          },
          {
            "name": "vortex-pushdown-compressed/exec",
            "value": 3088349.1664705877,
            "unit": "ns",
            "range": 5615.270838235272
          },
          {
            "name": "vortex-pushdown-uncompressed/planning",
            "value": 512353.7491326764,
            "unit": "ns",
            "range": 624.8239423614577
          },
          {
            "name": "vortex-pushdown-uncompressed/exec",
            "value": 2944284.9370588227,
            "unit": "ns",
            "range": 2915.2811985297594
          },
          {
            "name": "vortex-nopushdown-compressed/planning",
            "value": 721484.9883103865,
            "unit": "ns",
            "range": 484.4758781148121
          },
          {
            "name": "vortex-nopushdown-compressed/exec",
            "value": 13374309.42,
            "unit": "ns",
            "range": 16419.964562499896
          },
          {
            "name": "vortex-nopushdown-uncompressed/planning",
            "value": 720177.9309094431,
            "unit": "ns",
            "range": 1088.4671169615467
          },
          {
            "name": "vortex-nopushdown-uncompressed/exec",
            "value": 1684453.355530756,
            "unit": "ns",
            "range": 906.7713363126386
          }
        ]
      }
    ],
    "Vortex Compression": [
      {
        "commit": {
          "author": {
            "email": "dan@spiraldb.com",
            "name": "Dan King",
            "username": "danking"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "80c23d50b6a3730eb606953ad72ad0650312bd89",
          "message": "fix: version -> benchmark in GHA bench.yml (#893)\n\nI renamed this in bench-pr.yml and changed the references to the matrix\r\nvariable in both files, but forgot to rename it here in bench.yml",
          "timestamp": "2024-09-20T11:19:02-04:00",
          "tree_id": "6af96dd7d72f3764f2a6959809024555610ae461",
          "url": "https://github.com/spiraldb/vortex/commit/80c23d50b6a3730eb606953ad72ad0650312bd89"
        },
        "date": 1726846873921,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Yellow Taxi Trip Data Compression Time/taxi compression",
            "value": 2495489852.9,
            "unit": "ns",
            "range": 5324639.5
          },
          {
            "name": "Yellow Taxi Trip Data Compression Time/taxi compression throughput",
            "value": 470808924,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Yellow Taxi Trip Data Vortex-to-ParquetZstd Ratio/taxi",
            "value": 0.950720132370233,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Yellow Taxi Trip Data Vortex-to-ParquetUncompressed Ratio/taxi",
            "value": 0.6102863396038999,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Yellow Taxi Trip Data Compression Ratio/taxi",
            "value": 0.108380354319707,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Yellow Taxi Trip Data Compression Size/taxi",
            "value": 51026438,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Compression Time/AirlineSentiment compression",
            "value": 414285.3202901558,
            "unit": "ns",
            "range": 407.49095674967975
          },
          {
            "name": "Public BI Compression Time/AirlineSentiment compression throughput",
            "value": 2020,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetZstd Ratio/AirlineSentiment",
            "value": 6.400830737279335,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetUncompressed Ratio/AirlineSentiment",
            "value": 4.353107344632768,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Ratio/AirlineSentiment",
            "value": 0.6207920792079208,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Size/AirlineSentiment",
            "value": 1254,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Compression Time/Arade compression",
            "value": 3167474224,
            "unit": "ns",
            "range": 8338450.532500029
          },
          {
            "name": "Public BI Compression Time/Arade compression throughput",
            "value": 787023760,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetZstd Ratio/Arade",
            "value": 0.49183013997883884,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetUncompressed Ratio/Arade",
            "value": 0.43899818060510326,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Ratio/Arade",
            "value": 0.18677686300093405,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Size/Arade",
            "value": 146997829,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Compression Time/Bimbo compression",
            "value": 22609347933.1,
            "unit": "ns",
            "range": 21958403.818748474
          },
          {
            "name": "Public BI Compression Time/Bimbo compression throughput",
            "value": 7121333608,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetZstd Ratio/Bimbo",
            "value": 1.2947888412478443,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetUncompressed Ratio/Bimbo",
            "value": 0.8779098843621267,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Ratio/Bimbo",
            "value": 0.06426202635499392,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Size/Bimbo",
            "value": 457631328,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Compression Time/CMSprovider compression",
            "value": 13440398203.5,
            "unit": "ns",
            "range": 26367885.942500114
          },
          {
            "name": "Public BI Compression Time/CMSprovider compression throughput",
            "value": 5149123964,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetZstd Ratio/CMSprovider",
            "value": 1.2015352627069367,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetUncompressed Ratio/CMSprovider",
            "value": 0.7758227798964876,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Ratio/CMSprovider",
            "value": 0.1759921383395927,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Size/CMSprovider",
            "value": 906205337,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Compression Time/Euro2016 compression",
            "value": 2221059535.5,
            "unit": "ns",
            "range": 17257836.180000067
          },
          {
            "name": "Public BI Compression Time/Euro2016 compression throughput",
            "value": 393253221,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetZstd Ratio/Euro2016",
            "value": 1.4734606471495095,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetUncompressed Ratio/Euro2016",
            "value": 0.6251573935832805,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Ratio/Euro2016",
            "value": 0.4338117856128126,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Size/Euro2016",
            "value": 170597882,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Compression Time/Food compression",
            "value": 1099698480.6,
            "unit": "ns",
            "range": 3283747.3000000715
          },
          {
            "name": "Public BI Compression Time/Food compression throughput",
            "value": 332718229,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetZstd Ratio/Food",
            "value": 1.2275612994918959,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetUncompressed Ratio/Food",
            "value": 0.6940930688896351,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Ratio/Food",
            "value": 0.13019563770279627,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Size/Food",
            "value": 43318462,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Compression Time/HashTags compression",
            "value": 2942846431,
            "unit": "ns",
            "range": 13470919.942500114
          },
          {
            "name": "Public BI Compression Time/HashTags compression throughput",
            "value": 804495592,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetZstd Ratio/HashTags",
            "value": 1.6539300656954907,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Vortex-to-ParquetUncompressed Ratio/HashTags",
            "value": 0.4702155832326322,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Ratio/HashTags",
            "value": 0.26188628016746174,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "Public BI Compression Size/HashTags",
            "value": 210686358,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Compression Time/chunked-without-fsst compression",
            "value": 193243963.4165873,
            "unit": "ns",
            "range": 602588.1973016113
          },
          {
            "name": "TPC-H l_comment Compression Time/chunked-without-fsst compression throughput",
            "value": 183010921,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Vortex-to-ParquetZstd Ratio/chunked-without-fsst",
            "value": 3.2154753234004985,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Vortex-to-ParquetUncompressed Ratio/chunked-without-fsst",
            "value": 0.9983650596000513,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Compression Ratio/chunked-without-fsst",
            "value": 0.999965750677797,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Compression Size/chunked-without-fsst",
            "value": 183004653,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Compression Time/chunked-with-fsst compression",
            "value": 1123679440.9,
            "unit": "ns",
            "range": 813616.8756250143
          },
          {
            "name": "TPC-H l_comment Compression Time/chunked-with-fsst compression throughput",
            "value": 183010921,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Vortex-to-ParquetZstd Ratio/chunked-with-fsst",
            "value": 1.505559811115435,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Vortex-to-ParquetUncompressed Ratio/chunked-with-fsst",
            "value": 0.4674575791693884,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Compression Ratio/chunked-with-fsst",
            "value": 0.4433667376604263,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Compression Size/chunked-with-fsst",
            "value": 81140955,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Compression Time/canonical-with-fsst compression",
            "value": 1122060074.65,
            "unit": "ns",
            "range": 769123.1031249762
          },
          {
            "name": "TPC-H l_comment Compression Time/canonical-with-fsst compression throughput",
            "value": 183010937,
            "unit": "bytes",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Vortex-to-ParquetZstd Ratio/canonical-with-fsst",
            "value": 1.5026367800563647,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Vortex-to-ParquetUncompressed Ratio/canonical-with-fsst",
            "value": 0.4665463348275034,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Compression Ratio/canonical-with-fsst",
            "value": 0.44238636404555426,
            "unit": "ratio",
            "range": 0
          },
          {
            "name": "TPC-H l_comment Compression Size/canonical-with-fsst",
            "value": 80961543,
            "unit": "bytes",
            "range": 0
          }
        ]
      }
    ]
  }
}