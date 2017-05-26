var appId = ($("#realtime-info").attr("data-appId"));
var mqttPort = ($("#realtime-info").attr("data-mqttPort"));

var client = new Paho.MQTT.Client("localhost", Number(mqttPort), "webclient-"+appId);
var firstEntry;
var realtimeData = [];
var metricToDisplay;
var colorsMap = {};

// set callback handlers
client.onConnectionLost = onConnectionLost;
client.onMessageArrived = onMessageArrived;

// connect the client
client.connect({onSuccess:onConnect});

// called when the client connects
function onConnect() {
    // Once a connection has been made, make a subscription and send a message.
    console.log("onConnect");
    client.subscribe("metrics-"+appId);
}

// called when the client loses its connection
function onConnectionLost(responseObject) {
    if (responseObject.errorCode !== 0) {
        console.log("onConnectionLost:"+responseObject.errorMessage);
    }
}

// called when a message arrives
function onMessageArrived(message) {
    var entry = JSON.parse(message.payloadString).values;
    if(!firstEntry) {
        discoverTags(entry,"");
        availableKeys = (Object.keys(availableTags));
        availableKeys.sort();
        $.each(availableKeys, function(key, value) {
            $('#executor-metric-option')
                .append($("<option></option>")
                    .attr("value",value)
                    .text(value));
        });
        $('#executor-metric-option').on('change', function() {
            if(!(this.value==='NULL')) {
                metricToDisplay = (this.value);
                $('#executor-metrics').empty();
                renderData();
            }
            else {
                metricToDisplay = undefined;
                $('#executor-metrics').empty();
            }
        });
        firstEntry = entry;
    }
    realtimeData.push(JSON.parse(message.payloadString));
    renderData();
}

function renderData() {
    if(!metricToDisplay) return;

    var keyArr = metricToDisplay.split(".");
    var lastHost;
    var hostsProcessed = [];
    var metricsMap = {};
    var legend = [];
    var data = [];
    var colors = [];

    $.each(realtimeData, function (key, value) {

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

      /*  if (minimumDataTime == 0 || millis < minimumDataTime) {
            minimumDataTime = millis;
        }*/

        var entryObj = value.values;
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

    $.each(legend, function (ind, value) {
        data.push(metricsMap[value].data);
        if(!(value in colorsMap))
        {
            colorsMap[value] = getRandomColor();
        }
        colors.push(colorsMap[value]);
    });

    console.log(data);

    var graph = {
        title: metricToDisplay,
        data: data,
        full_width: true,
        area: false,
/*        animate_on_load: true,*/
        right: 100,
        missing_is_zero: false,
        min_y: -1,
        transition_on_update: false,
        y_extended_ticks: true,
        height: 300,
        colors: colors,
       // markers: markers,
        mouseover: function(d, i) {
            // custom format the rollover text, show days
            var prefix = d3.formatPrefix(d.value);
            var display = d3.select('#executor-metrics svg .mg-active-datapoint')
            display.text("Value:");
            display.append("tspan").text(Math.round(d.value * 100) / 100).style("font-weight","bold");
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

    //if(metricToDisplay in tooltips) graph.description = tooltips[inputKey];

    $(graph.target).empty();

    MG.data_graphic(graph);

}