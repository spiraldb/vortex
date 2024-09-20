window.BENCHMARK_DATA = {
  "lastUpdate": 1726845974135,
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
    ]
  }
}