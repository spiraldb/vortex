'use strict';
window.initAndRender = (function () {
    function stringToColor(str) {
        // Random colours are generally pretty disgusting...
        const MAP = {
            "vortex-file-uncompressed": '#98da8d',
            "vortex-file-compressed": '#23d100',
            "vortex-in-memory-no-pushdown": '#79a6df',
            "vortex-in-memory-pushdown": '#0c53ae',
            "arrow": '#58067e',
            "parquet": '#ef7f1d',
        };

        if (MAP[str]) {
            return MAP[str];
        } else {
            console.log("Using random color for: " + str)
        }

        var hash = new Hashes.MD5().hex(str)

        // Return a CSS color string
        const hexColor = hash.slice(0, 2) + hash.slice(14, 16) + hash.slice(30, 32);
        return `#${hexColor}`;
    }

    function init() {
        function collectBenchesPerTestCase(entries) {
            // It's desirable for all our graphs to line up in terms of X-axis.
            // As such, we collect all unique {commit,entry} first, and then assign
            // data points to them for each graph. Commits are sorted by date.
            const commits = [];
            const dates = [];
            entries.sort((a, b) => new Date(a.commit.timestamp) - new Date(b.commit.timestamp)).forEach(entry => {
                commits.push(entry.commit);
                dates.push(entry.date);
            });


            const map = new Map();
            entries.forEach((entry, entryIdx) => {
                const {tool, benches} = entry;

                benches.forEach((bench, benchIdx) => {
                    let {name, range, unit, value} = bench;

                    // Normalize name and units
                    let [q, seriesName] = name.split("/");
                    if (seriesName.endsWith(" throughput")) {
                        seriesName = seriesName.slice(0, seriesName.length - " throughput".length);
                        q = q.replace("time", "throughput");
                        let timeEntry = benches[benchIdx - 1]
                        if (!timeEntry.name.includes(seriesName)) {
                            console.log("could not find time information for throughput series: ", seriesNam);
                            return
                        }
                        unit = "bytes/ns"
                        value = value / timeEntry.value
                    }
                    let prettyQ = q.replace("_", " ")
                        .toUpperCase();
                    if (prettyQ.includes("PARQUET-UNC")) {
                        return
                    }

                    const is_nanos = unit === "ns/iter" || unit === "ns";
                    const is_bytes = unit === "bytes";
                    const is_throughput = unit === "bytes/ns";

                    let sort_position = (q.slice(0, 4) == "tpch") ? parseInt(prettyQ.split(" ")[1].substring(1), 10) : 0;

                    let arr = map.get(prettyQ);
                    if (arr === undefined) {
                        map.set(prettyQ, {
                            sort_position,
                            commits,
                            unit: is_nanos ? "ms/iter" : (is_bytes ? "MiB" : (is_throughput ? "MiB/s" : unit)),
                            series: new Map(),
                        });
                        arr = map.get(prettyQ);
                    }

                    let series = arr.series.get(seriesName);
                    if (series === undefined) {
                        arr.series.set(seriesName, new Array(entries.length).fill(null));
                        series = arr.series.get(seriesName);
                    }

                    series[entryIdx] = {range, value: is_nanos ? value / 1_000_000 : (is_bytes ? value / 1_048_576 : (is_throughput ? value * 1_000_000_000 / 1_048_576 : value))};
                });
            });

            function sortByPositionThenName(a, b) {
                let position_compare = a[1].sort_position - b[1].sort_position
                if (position_compare !== 0) {
                    return position_compare
                }
                return a[0].localeCompare(b[0]);
            }
            return new Map([...map.entries()].sort(sortByPositionThenName));
        }

        const data = window.BENCHMARK_DATA;

        // Render header
        document.getElementById('last-update').textContent = new Date(data.lastUpdate).toString();
        const repoLink = document.getElementById('repository-link');
        repoLink.href = data.repoUrl;
        repoLink.textContent = data.repoUrl;

        // Render footer
        document.getElementById('dl-button').onclick = () => {
            const dataUrl = 'data:,' + JSON.stringify(data, null, 2);
            const a = document.createElement('a');
            a.href = dataUrl;
            a.download = 'benchmark_data.json';
            a.click();
        };

        // Prepare data points for charts
        return Object.keys(data.entries).map(name => ({
            name,
            dataSet: collectBenchesPerTestCase(data.entries[name]),
        }));
    }

    function renderAllCharts(dataSets, keptGroups) {

        var charts = [];

        function renderChart(parent, name, dataset, hiddenDatasets, removedDatasets) {
            const canvasContainer = document.createElement('div');
            parent.appendChild(canvasContainer);

            const canvas = document.createElement('canvas');
            canvas.className = 'benchmark-chart';
            canvasContainer.appendChild(canvas);

            const data = {
                labels: dataset.commits.map(commit => commit.id.slice(0, 7)),
                datasets: Array.from(dataset.series).filter(([name, benches]) => {
                    return removedDatasets === undefined || !removedDatasets.has(name)
                }).map(([name, benches]) => {
                    const color = stringToColor(name);
                    return {
                        label: name,
                        data: benches.map(b => b ? b.value : null),
                        borderColor: color,
                        backgroundColor: color + '60', // Add alpha for #rrggbbaa
                        hidden: hiddenDatasets !== undefined && hiddenDatasets.has(name),
                    };
                }),
            };
            const y_axis_scale = {
                title: {
                    display: true,
                    text: dataset.commits.length > 0 ? dataset.unit : '',
                },
                suggestedMin: 0,
            }

            if (name.includes("COMPRESS THROUGHPUT") && dataset.unit == "MiB/s") {
                y_axis_scale.suggestedMax = 1000;
                y_axis_scale.max = 1000;
            }

            if (name.includes("DECOMPRESS THROUGHPUT") && dataset.unit == "MiB/s") {
                y_axis_scale.suggestedMax = 2750;
                y_axis_scale.max = 2750;
            }

            const options = {
                responsive: true,
                maintainAspectRatio: false,
                spanGaps: true,
                pointStyle: 'crossRot',
                elements: {
                    line: {
                        borderWidth: 1,
                    },

                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: name,
                            padding: {bottom: 50},
                        },
                        // By default, show the last 50 commits
                        min: Math.max(0, dataset.commits.length - 50),
                    },
                    y: y_axis_scale,
                },
                plugins: {
                    zoom: {
                        zoom: {
                            wheel: {enabled: true},
                            mode: 'x',
                            drag: {enabled: true}
                        }
                    },
                    legend: {
                        display: true,
                        onClick: function (e, legendItem) {
                            var index = legendItem.datasetIndex;

                            var wasVisible = this.chart.isDatasetVisible(index);
                            var datasetLabel = this.chart.data.datasets[index].label;
                            var clickedChart = this.chart;

                            charts.forEach(function(chart) {
                                chart.data.datasets.forEach(function(ds, idx) {
                                    if (ds.label === datasetLabel) {
                                        chart.getDatasetMeta(idx).hidden = wasVisible;
                                    }
                                });

                                chart.update();
                            });
                        }
                    },
                    tooltip: {
                        callbacks: {
                            footer: items => {
                                const {dataIndex} = items[0];
                                const commit = dataset.commits[dataIndex];
                                return commit.message.split("\n")[0] + "\n" + commit.author.username;
                            }
                        }
                    }
                },
                onClick: (_mouseEvent, activeElems) => {
                    if (activeElems.length === 0) {
                        return;
                    }
                    // XXX: Undocumented. How can we know the index?
                    const index = activeElems[0].index;
                    const url = dataset.commits[index].url;
                    window.open(url, '_blank');
                },
            };

            return new Chart(canvas, {
                type: 'line',
                data,
                options,
            });
        }

        function renderBenchSet(name, benchSet, main, toc, groupFilterSettings) {
            const {keptCharts, hiddenDatasets, removedDatasets} = groupFilterSettings;
            const setElem = document.createElement('div');
            setElem.className = 'benchmark-set';
            main.appendChild(setElem);

            const h1id = name.replace(" ", "_");
            const nameElem = document.createElement('h1');
            nameElem.id = h1id;
            nameElem.className = 'benchmark-title';
            nameElem.textContent = name;
            setElem.appendChild(nameElem);

            const tocLi = document.createElement('li');
            const tocLink = document.createElement('a');
            tocLink.href = '#' + h1id;
            tocLink.innerHTML = name;
            tocLi.appendChild(tocLink);
            toc.appendChild(tocLi);

            const graphsElem = document.createElement('div');
            graphsElem.className = 'benchmark-graphs';
            setElem.appendChild(graphsElem);

            if (keptCharts == undefined) {
                for (const [benchName, benches] of benchSet.entries()) {
                    charts.push(renderChart(graphsElem, benchName, benches, hiddenDatasets, removedDatasets))
                }
            } else {
                for (const benchName of keptCharts) {
                    const benches = benchSet.get(benchName)
                    charts.push(renderChart(graphsElem, benchName, benches, hiddenDatasets, removedDatasets))
                }
            }
        }

        const main = document.getElementById('main');
        const toc = document.getElementById('toc');
        for (const {name, dataSet} of dataSets) {
            if (keptGroups === undefined) {
                renderBenchSet(name, dataSet, main, toc, {
                    "keptCharts": undefined,
                    "hiddenDatasets": undefined,
                    "removedDatasets": undefined
                });
            } else if (keptGroups.get(name)) {
                renderBenchSet(name, dataSet, main, toc, keptGroups.get(name));
            }
        }
    }

    function initAndRender(keptGroups) {
        renderAllCharts(init(), keptGroups);
    };

    return initAndRender;
})();
