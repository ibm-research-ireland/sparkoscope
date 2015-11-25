var minimumDataTime = 0;

var markers = [];
var minimumSubmittedValue = 0;

var executorMetrics = [];

var availableTags = {};
var availableKeys = [];
var stageInfo = [];
var jobInfo = [];
var tooltips = {};

function parseExecutorMetrics(_executorMetrics,_stageInfo,_jobInfo,_tooltips) {
    executorMetrics = _executorMetrics;
    stageInfo = _stageInfo;
    jobInfo = _jobInfo;
    tooltips = _tooltips;
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
    var hostsProcessed = [];
    var lastHost;
    markers = [];

    for (var x in jobInfo) {
        var submitted = jobInfo[x].submitted;
        if (minimumSubmittedValue == 0 || submitted < minimumSubmittedValue) {
            minimumSubmittedValue = submitted;
        }
        markers.push({
            date: new Date(submitted)
        })
    }

    $.each(executorMetrics, function (key, value) {

        var host = value.host;
        if(keyArr[0]=='sigar') {
            var splitted = host.split("_");
            host = splitted.slice(0,splitted.length-1).join("_")
        }
        if(lastHost) {
            if(lastHost!=host)
            {
                if(host in hostsProcessed) return false;
                else {
                    hostsProcessed.push(lastHost);
                    lastHost = host;
                }
            } else {
                if(host in hostsProcessed) return false;
            }
        }
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

        var entryToAdd = {
            date: new Date(millis),
            value: metric,
            host: host
        };

        metricsMap[host].data.push(entryToAdd);
    });
    $.each(metricsMap, function(key,value){
        if (minimumSubmittedValue < minimumDataTime) {
            var placeholder = {
                date: new Date(minimumSubmittedValue - 1000),
                value: metricsMap[key].data[0].value,
                host: metricsMap[key].data[0].host
            };
            metricsMap[key].data.unshift(placeholder);
        }
    });

    var colors = [];
    var stageInfoByTimes = [];
    var jobInfoByTimes = [];

    $.each(stageInfo, function(ind, value){
        stageInfoByTimes.push(value.submitted)
    });
    $.each(jobInfo, function(ind, value){
        jobInfoByTimes.push(value.submitted)
    });

    var max = function (arr) { return  Math.max.apply(null, arr); };
    var min = function (arr) { return  Math.min.apply(null, arr); };

    var nearest  = function (arr, x) {
        var l = [], h = [];

        arr.forEach(function (v) {
            ((v < x) && l.push(v)) || ((v > x) && h.push(v));
        });

        return {
            "low" : arr.indexOf(max(l)),
            "high": arr.indexOf(min(h))
        };
    };

    $.each(legend, function (ind, value) {
        $.each(metricsMap[value].data, function(i,val){
            var metricTime = val.date.getTime();

            var stageIndexObj = (nearest(stageInfoByTimes, metricTime));
            var stageIndex = alignIndex(stageIndexObj,stageInfoByTimes,metricTime);
            metricsMap[value].data[i].stage = stageIndex;

            var jobIndexObj = (nearest(jobInfoByTimes, metricTime));
            var jobIndex = alignIndex(jobIndexObj,jobInfoByTimes, metricTime);
            metricsMap[value].data[i].job = jobIndex;
        });

        data.push(metricsMap[value].data);
        colors.push(getRandomColor());
    });

    var graph = {
        title: inputKey,
        data: data,
        full_width: true,
        area: false,
        animate_on_load: true,
        right: 100,
        missing_is_zero: false,
        min_y: -1,
        y_extended_ticks: true,
        height: 300,
        colors: colors,
        markers: markers,
        mouseover: function(d, i) {
            // custom format the rollover text, show days
            var prefix = d3.formatPrefix(d.value);
            var display = d3.select('#executor-metrics svg .mg-active-datapoint')
            display.text("Value:");
            display.append("tspan").text(Math.round(d.value * 100) / 100).style("font-weight","bold");
            display.append("tspan").text(" Job:")
            display.append("tspan").text(d.job).style("font-weight","bold");
            display.append("tspan").text(" Stage:");
            display.append("tspan").text(d.stage).style("font-weight","bold");
            var splitted = d.host.split("_");
            if(splitted.length>1){
                display.append("tspan").text(" Host:")
                display.append("tspan").text(splitted.slice(0,splitted.length-1).join("_")).style("font-weight","bold");
                display.append("tspan").text(" Executor:")
                display.append("tspan").text(splitted[splitted.length-1]).style("font-weight","bold");
            } else {
                display.append("tspan").text(" Host:")
                display.append("tspan").text(d.host).style("font-weight","bold");
            }
        },
        target: '#executor-metrics'
    };

    if(inputKey in tooltips) graph.description = tooltips[inputKey];

    $(graph.target).empty();

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

function getRandomColor() {
    var letters = '0123456789ABCDEF'.split('');
    var color = '#';
    for (var i = 0; i < 6; i++ ) {
        color += letters[Math.floor(Math.random() * 16)];
    }
    return color;
}

function alignIndex(indexObj,arr,val)
{
    if(indexObj.low==-1) return 0;
    if(indexObj.high==-1) {
        return arr.length-1;
    }
    return indexObj.low;
}