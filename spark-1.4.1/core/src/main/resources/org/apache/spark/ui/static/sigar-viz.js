var networkData = [];
var networkMap = {};

var networkRxData = [];
var networkRxMap = {};

var networkTxData = [];
var networkTxMap = {};

var diskData = [];
var diskMap = {};

var diskWrittenData = [];
var diskWrittenMap = {};

var diskReadData = [];
var diskReadMap = {};

var cpuData = [];
var cpuMap = {};

var ramData = [];
var ramMap = {};

var legend = [];
var minimumDataTime = 0;

var markers = [];
var minimumSubmittedValue = 0;
var minimumSubmittedIndex = 0;

function drawSigarMetrics(sigarMetrics, stageInfo) {

    for (var x in stageInfo) {
        var name = stageInfo[x].name;
        var submitted = new Date(stageInfo[x].submitted);
        if (minimumSubmittedValue == 0 || submitted < minimumSubmittedValue) {
            minimumSubmittedValue = submitted;
            minimumSubmittedIndex = markers.length;
        }
        markers.push({
            date: submitted,
            label: name
        })
    }

    for (var x in sigarMetrics) {
        var host = sigarMetrics[x].host;
        var millis = sigarMetrics[x].timestamp;

        if (minimumDataTime == 0 || millis < minimumDataTime) {
            minimumDataTime = millis;
        }

        networkMap[host] = appendEntry(sigarMetrics[x], 'totalNetwork', host, millis);
        networkRxMap[host] = appendEntry(sigarMetrics[x], 'kBytesRxPerSecond', host, millis);
        networkTxMap[host] = appendEntry(sigarMetrics[x], 'kBytesTxPerSecond', host, millis);

        diskMap[host] = appendEntry(sigarMetrics[x], 'totalDisk', host, millis);
        diskWrittenMap[host] = appendEntry(sigarMetrics[x], 'kBytesWrittenPerSecond', host, millis);
        diskReadMap[host] = appendEntry(sigarMetrics[x], 'kBytesReadPerSecond', host, millis);

        cpuMap[host] = appendEntry(sigarMetrics[x], 'cpu', host, millis);

        ramMap[host] = appendEntry(sigarMetrics[x], 'ram', host, millis);
    }
    for (var host in networkMap) {

        if (minimumSubmittedValue < minimumDataTime) {
            var placeholder = {
                date: new Date(minimumSubmittedValue - 3000),
                value: 0.0
            };
            networkMap[host].push(placeholder);
            networkRxMap[host].push(placeholder);
            networkTxMap[host].push(placeholder);
            diskMap[host].push(placeholder);
            diskReadMap[host].push(placeholder);
            diskWrittenMap[host].push(placeholder);
            cpuMap[host].push(placeholder);
            ramMap[host].push(placeholder);
        }

        legend.push(host);
        networkData.push(networkMap[host]);
        networkRxData.push(networkRxMap[host]);
        networkTxData.push(networkTxMap[host]);
        diskData.push(diskMap[host]);
        diskWrittenData.push(diskWrittenMap[host]);
        diskReadData.push(diskReadMap[host]);
        cpuData.push(cpuMap[host]);
        ramData.push(ramMap[host]);
    }

    var networkGraph = createChartByMode('totalNetwork');
    MG.data_graphic(networkGraph);
    addEventListener("network", networkGraph);

    var diskGraph = createChartByMode('totalDisk');
    MG.data_graphic(diskGraph);
    addEventListener("disk", diskGraph);

    var cpuGraph = createChartByMode('cpu');
    MG.data_graphic(cpuGraph);
    addEventListener("cpu", cpuGraph);

    var ramGraph = createChartByMode('ram');
    MG.data_graphic(ramGraph);
    addEventListener("ram", ramGraph);

    $('#networkMode').on('change', function() {
        var graph = createChartByMode(this.value);
        graph.full_width = true;
        MG.data_graphic(graph);
    });

    $('#diskMode').on('change', function() {
        var graph = createChartByMode(this.value);
        graph.full_width = true;
        MG.data_graphic(graph);
    });
}

function addEventListener(tag, graph) {
    $("span.expand-" + tag).click(function() {
        var status = ($("#sigar-" + tag + "-metrics-container").css('display'));
        var statusNew = ($("#sigar-" + tag + "-metrics").css('display'));
        $("#sigar-" + tag + "-metrics-container").toggleClass('collapsed');
        if (status == 'none') {
            if (!graph.full_width) {
                graph.full_width = true;
                MG.data_graphic(graph);
            }
        }
        // Switch the class of the arrow from open to closed.
        $(this).find('.expand-' + tag + '-arrow').toggleClass('arrow-open');
        $(this).find('.expand-' + tag + '-arrow').toggleClass('arrow-closed');
    });
}

function appendEntry(sigarMetricsRow, mode, host, millis) {

    var existingData = [];
    if (mode == 'totalNetwork') {
        if (host in networkMap) {
            existingData = networkMap[host];
        }
    } else if (mode == 'kBytesRxPerSecond') {
        if (host in networkRxMap) {
            existingData = networkRxMap[host];
        }
    } else if (mode == 'kBytesTxPerSecond') {
        if (host in networkTxMap) {
            existingData = networkTxMap[host];
        }
    } else if (mode == 'totalDisk') {
        if (host in diskMap) {
            existingData = diskMap[host];
        }
    } else if (mode == 'kBytesWrittenPerSecond') {
        if (host in diskWrittenMap) {
            existingData = diskWrittenMap[host];
        }
    } else if (mode == 'kBytesReadPerSecond') {
        if (host in diskReadMap) {
            existingData = diskReadMap[host];
        }
    } else if (mode == 'cpu') {
        if (host in cpuMap) {
            existingData = cpuMap[host];
        }
    } else if (mode == 'ram') {
        if (host in ramMap) {
            existingData = ramMap[host];
        }
    }
    var value = 0.0;
    if (mode == 'totalNetwork') {
        value = parseFloat(sigarMetricsRow.kBytesRxPerSecond) + parseFloat(sigarMetricsRow.kBytesTxPerSecond);
    } else if (mode == 'kBytesRxPerSecond') {
        value = parseFloat(sigarMetricsRow.kBytesRxPerSecond);
    } else if (mode == 'kBytesTxPerSecond') {
        value = parseFloat(sigarMetricsRow.kBytesTxPerSecond);
    } else if (mode == 'totalDisk') {
        value = parseFloat(sigarMetricsRow.kBytesWrittenPerSecond) + parseFloat(sigarMetricsRow.kBytesReadPerSecond);
    } else if (mode == 'kBytesWrittenPerSecond') {
        value = parseFloat(sigarMetricsRow.kBytesWrittenPerSecond);
    } else if (mode == 'kBytesReadPerSecond') {
        value = parseFloat(sigarMetricsRow.kBytesReadPerSecond);
    } else if (mode == 'cpu') {
        value = parseFloat(sigarMetricsRow.cpu);
    } else if (mode == 'ram') {
        value = parseFloat(sigarMetricsRow.ram);
    }

    existingData.push({
        date: new Date(millis),
        value: value
    });
    return existingData;
}

function createChartByMode(mode) {
    var graph = {
        title: "",
        description: "",
        data: [],
        area: false,
        right: 100,
        width: 300,
        missing_is_zero: false,
        interpolate: 'basic',
        min_y: -1,
        y_extended_ticks: true,
        height: 300,
        legend: legend,
        markers: markers,
        target: ''
    };

    if (mode == 'totalNetwork') {
        graph.title = "Total Network Traffic";
        graph.description = "Sum of kBytesRxPerSecond and kBytesTxPerSecond per host";
        graph.data = networkData;
        graph.target = '#sigar-network-metrics';
    } else if (mode == 'kBytesRxPerSecond') {
        graph.title = "Incoming Network Traffic";
        graph.description = "kBytesRxPerSecond per host";
        graph.data = networkRxData;
        graph.target = '#sigar-network-metrics';
    } else if (mode == 'kBytesTxPerSecond') {
        graph.title = "Outgoing Network Traffic";
        graph.description = "kBytesTxPerSecond per host";
        graph.data = networkTxData;
        graph.target = '#sigar-network-metrics';
    } else if (mode == 'totalDisk') {
        graph.title = "Total Disk IO";
        graph.description = "Sum of kBytes Written and Read per host";
        graph.data = diskData;
        graph.target = '#sigar-disk-metrics';
    } else if (mode == 'kBytesWrittenPerSecond') {
        graph.title = "Disk Writes";
        graph.description = "kBytes Written per host";
        graph.data = diskWrittenData;
        graph.target = '#sigar-disk-metrics';
    } else if (mode == 'kBytesReadPerSecond') {
        graph.title = "Disk Reads";
        graph.description = "kBytes Read per host";
        graph.data = diskReadData;
        graph.target = '#sigar-disk-metrics';
    } else if (mode == 'cpu') {
        graph.title = "CPU Utilization";
        graph.description = "Percentage of CPU Utilization per host";
        graph.data = cpuData;
        graph.target = '#sigar-cpu-metrics';
    } else if (mode == 'ram') {
        graph.title = "RAM Utilization";
        graph.description = "Percentage of RAM Utilization per host";
        graph.data = ramData;
        graph.target = '#sigar-ram-metrics';
    }

    return graph;
}