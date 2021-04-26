package qupath.lib.gui.measure;

import net.mahdilamb.dataframe.BooleanSeries;
import net.mahdilamb.dataframe.DataFrame;
import net.mahdilamb.dataframe.DoubleSeries;
import net.mahdilamb.dataframe.Series;
import qupath.lib.objects.PathObject;

import java.util.List;
import java.util.function.Predicate;

public final class QuPathDataFrame implements DataFrame {
    private final DataFrame dataFrame;
    private final Series<? extends Comparable<?>>[] series;
    private final List<PathObject> objs;

    public QuPathDataFrame(List<PathObject> objects) {
        this.objs = objects;
        final DoubleSeries[] series = new DoubleSeries[objs.get(0).getMeasurementList().size()];
        for (int j = 0; j < series.length; ++j) {
            int finalJ = j;
            series[j] = Series.of(objs.get(0).getMeasurementList().getMeasurementName(j), i -> objs.get(i).getMeasurementList().getMeasurementValue(finalJ), objs.size());
        }
        this.series = series;
        dataFrame = DataFrame.from("", series);
    }

    @Override
    public String getName() {
        return dataFrame.getName();
    }

    @Override
    public int numSeries() {
        return dataFrame.numSeries();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Series<Comparable<Object>> get(int series) {
        return (Series<Comparable<Object>>) this.series[series];
    }

    @Override
    public DataFrame subsetCols(String... names) {
        return dataFrame.subsetCols(names);
    }

    @Override
    public DataFrame subsetCols(int start, int end) {
        return dataFrame.subsetCols(start, end);
    }

    @Override
    public DataFrame subsetCols(Predicate<String> test) {
        return dataFrame.subsetCols(test);
    }

    @Override
    public DataFrame filter(BooleanSeries filter) {
        return dataFrame.filter(filter);
    }

    @Override
    public DataFrame subset(int start, int end) {
        return dataFrame.subset(start, end);
    }

    @Override
    public <S extends Comparable<S>> DataFrame filter(String series, Predicate<S> test) {
        return dataFrame.filter(series, test);
    }

    @Override
    public DataFrame query(String query) {
        return dataFrame.query(query);
    }

    @Override
    public DataFrame sortBy(int index, boolean ascending) {
        return dataFrame.sortBy(index, ascending);
    }

    @Override
    public DataFrame sortBy(String name, boolean ascending) {
        return dataFrame.sortBy(name, ascending);
    }

    @Override
    public Iterable<DataFrame> groupBy(String name) {
        return dataFrame.groupBy(name);
    }

    @Override
    public String toString(int maxCols, int maxRows) {
        return dataFrame.toString(maxCols, maxRows);
    }

    @Override
    public String toString() {
        return dataFrame.toString();
    }

    @Override
    public List<String> listSeriesNames() {
        return objs.get(0).getMeasurementList().getMeasurementNames();
    }

}
