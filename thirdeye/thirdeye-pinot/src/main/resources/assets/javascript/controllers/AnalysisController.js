function AnalysisController(parentController) {
  this.parentController = parentController;
  this.analysisModel = new AnalysisModel();
  this.analysisView = new AnalysisView(this.analysisModel);
  this.timeSeriesCompareController = new TimeSeriesCompareController(this);
  this.percentageChangeTableController = new PercentageChangeTableController(this);
  this.dimensionTreeMapController = new DimensionTreeMapController(this);
}

AnalysisController.prototype = {
  handleAppEvent: function (hashParams) {
    this.analysisModel.init(hashParams);
    this.analysisModel.update();
    this.analysisView.init();
    this.analysisView.render();
    this.timeSeriesCompareController.handleAppEvent(hashParams);
    this.percentageChangeTableController.handleAppEvent(hashParams);
    this.dimensionTreeMapController.handleAppEvent(hashParams);
  }
};
