window.BENCHMARK_DATA = {
  "lastUpdate": 1726846073169,
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
    ]
  }
}