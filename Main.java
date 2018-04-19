import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static final int W = 12;       //moving variance is computed from i-W to i+W for index i
    public static final double T1 = 2.0;  //threshold value 1
    public static final double T2 = 1.0;  //threshold value 2

    public static void main(String[] args) throws IOException {
        List<double[]> accelerations = new ArrayList<double[]>();
        List<Double> magnitudes = new ArrayList<Double>();
        List<Double> movingStandardDeviation = new ArrayList<Double>();
        List<Integer> phases = new ArrayList<Integer>();
        //possible phases are : swing phase B1 (with value 1) when the moving variance is more than T1
        //                      stance phase B2 (with value 2) when the moving variance is less than T2
        //                      "in-between" phase (with value 0) when the moving variance is between T1 and T2

        int steps = 0;

        if(args.length < 1) {
            System.out.println("Error : no valid csv file path provided");
            return;
        }

        //We read the .csv file, and compute the magnitude of the acceleration vector at each sample
        BufferedReader reader = new BufferedReader(new FileReader(args[0]));
        try {
            String line = reader.readLine();

            while (line != null) {
                double[] emptyArr = {0.0, 0.0, 0.0};
                accelerations.add(emptyArr);
                phases.add(2);
                String[] coords = line.split(",");
                double sum = 0.0;
                for(int i = 0; i < 3; i++) {
                    accelerations.get(accelerations.size() - 1)[i] = Double.parseDouble(coords[i]);
                    sum += Math.pow(accelerations.get(accelerations.size() - 1)[i], 2);
                }
                magnitudes.add(Math.sqrt(sum));
                movingStandardDeviation.add(0.0);
                line = reader.readLine();
            }
        } finally {
            reader.close();
        }


        //We add a step each time the phase goes from swing to stance
        boolean hasBeenInSwingPhase = false;
        for(int i = W; i < accelerations.size() - W; i++) {
            movingStandardDeviation.set(i, Math.sqrt(movingVariance(magnitudes, W, i)));
            if(movingStandardDeviation.get(i) > T1) {
                phases.set(i, 1);   //swing phase
                hasBeenInSwingPhase = true;
            } else if (movingStandardDeviation.get(i) > T2) {
                phases.set(i, 0);   //in-between phase
            } else {
                if(hasBeenInSwingPhase) {
                    hasBeenInSwingPhase = false;
                    steps++;
                }
                phases.set(i, 2);   //stance phase
            }
        }
        System.out.println(steps);
        return;
    }

    //this function calculates the moving variance at a specified index, with a specified number of samples before
    // and after the index (as defined in lecture 2 slide 25)
    public static double movingVariance(List<Double> accelerations, int numberOfSamples, int index) {
        if((index - numberOfSamples) < 0 || (index + numberOfSamples) >= accelerations.size()) {
            System.out.println("Error : calculating moving variance at wrong index");
            return 0;
        }

        double sum = 0.0;
        for(int i = index - numberOfSamples; i <= index + numberOfSamples; i++) {
            sum += accelerations.get(i);
        }
        double average = sum / (2*numberOfSamples +1);

        sum = 0.0;
        for(int i = index - numberOfSamples; i <= index + numberOfSamples; i++) {
            sum += Math.pow(accelerations.get(i) - average, 2);
        }
        return sum /(2*numberOfSamples +1);
    }
}