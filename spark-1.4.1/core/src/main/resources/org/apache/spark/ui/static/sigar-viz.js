function drawSigarMetrics(sigarMetrics) {

  var data = [];
  var map = {};
  var legend = [];

  for (var x in sigarMetrics) {
    var host = sigarMetrics[x].host;
    var existingData = [];
    if(host in map)
    {
      existingData = map[host];
    }
    existingData.push( {
      date: new Date(sigarMetrics[x].timestamp),
      value: parseFloat(sigarMetrics[x].bytesRxPerSecond)+parseFloat(sigarMetrics[x].bytesTxPerSecond)
    });
    map[host] = existingData;
  }
  for (var host in map)
  {
    legend.push(host);
    data.push(map[host]);
  }

var graph = {
                    title: "Total Network Traffic",
                    description: "Sum of bytesRxPerSecond and bytesTxPerSecond per host",
                    data: data,
                    area: false,
                    right: 100,
                    width: 300,
                    missing_is_zero: false,
                    interpolate: 'basic',
                    min_y: -1,
                    y_extended_ticks: true,
                    height: 300,
                    legend: legend,
                    target: '#sigar-network-metrics'
                }

  MG.data_graphic(graph);

      $("span.expand-network").click(function() {
        var status = ($("#sigar-network-metrics").css('display'));
        $("#sigar-network-metrics").toggleClass('collapsed');
        if(status=='none')
        {
          graph.full_width = true;
          MG.data_graphic(graph);
        }


        // Switch the class of the arrow from open to closed.
        $(this).find('.expand-network-arrow').toggleClass('arrow-open');
        $(this).find('.expand-network-arrow').toggleClass('arrow-closed');
      });

}