var minimumDataTime = 0;

var markers = [];
var minimumSubmittedValue = 0;

var executorMetrics = [];

var availableTags = {};
var availableKeys = [];
var stageInfo = [];
var jobInfo = [];

function parseExecutorMetrics(_executorMetrics,_stageInfo,_jobInfo) {
    executorMetrics = _executorMetrics;
    stageInfo = _stageInfo;
    jobInfo = _jobInfo;
    var firstEntryObj = JSON.parse(executorMetrics[0].values);
    discoverTags(firstEntryObj, "");
    availableKeys = (Object.keys(availableTags));
    availableKeys.sort();
    $.each(availableKeys, function(key, value) {
        $('#executor-metric-option')
            .append($("<option></option>")
                .attr("value",value)
                .text(value));
    });
    $('#executor-metric-option').on('change', function() {
        if(!(this.value==='NULL')) createChartForTag(this.value);
        else $('#executor-metrics').empty();
    });
}

function createChartForTag(inputKey) {
    var keyArr = inputKey.split(".");
    var metricsMap = {};
    var legend = [];
    var data = [];
    markers = [];

    for (var x in stageInfo) {
        var name = stageInfo[x].name;
        var submitted = stageInfo[x].submitted;
        if (minimumSubmittedValue == 0 || submitted < minimumSubmittedValue) {
            minimumSubmittedValue = submitted;
        }
        markers.push({
            date: new Date(submitted),
            label: name
        })
    }

    $.each(executorMetrics, function (key, value) {
        var host = value.host;
        if (!(host in metricsMap)) {
            metricsMap[host] = {data: []};
            legend.push(host);
        }
        var millis = value.timestamp*1000;

        if (minimumDataTime == 0 || millis < minimumDataTime) {
            minimumDataTime = millis;
        }

        var entryObj = JSON.parse(value.values);
        var metric = entryObj[keyArr[0]];
        for (var i = 1; i < keyArr.length; i++) {
            metric = metric[keyArr[i]];
        }
        metricsMap[host].data.push({
            date: new Date(millis),
            value: metric
        });
    });
    $.each(metricsMap, function(key,value){
        if (minimumSubmittedValue < minimumDataTime) {
            var placeholder = {
                date: new Date(minimumSubmittedValue - 1000),
                value: metricsMap[key].data[0].value
            };
            metricsMap[key].data.unshift(placeholder);
        }
    });

    $.each(legend, function (ind, value) {
        data.push(metricsMap[value].data);
    });

    var graph = {
        title: inputKey,
        description: "",
        data: data,
        full_width: true,
        area: false,
        right: 100,
        missing_is_zero: false,
        interpolate: 'basic',
        min_y: -1,
        y_extended_ticks: true,
        height: 300,
        legend: legend,
        markers: markers,
        target: '#executor-metrics'
    };

    MG.data_graphic(graph);
}

function discoverTags(inputObj, parent) {
    $.each(inputObj, function (key, value) {
        var isObj = typeof value === 'object'
        if (isObj) {
            if (parent.length == 0) {
                availableTags[key] = {}
                discoverTags(value, key);
            } else {
                delete availableTags[parent];
                availableTags[parent + "." + key] = {};
                discoverTags(value, parent + "." + key);
            }
        } else {
            if (parent.length == 0) {
                availableTags[key] = {}
            } else {
                delete availableTags[parent];
                availableTags[parent + "." + key] = {};
            }
        }
    });
}