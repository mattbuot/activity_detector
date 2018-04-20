/*******************************************************************************
 * Copyright (c) 2009, 2014 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Dave Locke - initial API and implementation and/or initial documentation
 */

package org.eclipse.paho.sample.mqttv3app;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 * A sample application that demonstrates how to use the Paho MQTT v3.1 Client API in
 * non-blocking callback/notification mode.
 *
 * It can be run from the command line in one of two modes:
 *  - as a publisher, sending a single message to a topic on the server
 *  - as a subscriber, listening for messages from the server
 *
 *  There are three versions of the sample that implement the same features
 *  but do so using using different programming styles:
 *  <ol>
 *  <li>Sample which uses the API which blocks until the operation completes</li>
 *  <li>SampleAsyncWait shows how to use the asynchronous API with waiters that block until
 *  an action completes</li>
 *  <li>SampleAsyncCallBack (this one) shows how to use the asynchronous API where events are
 *  used to notify the application when an action completes<li>
 *  </ol>
 *
 *  If the application is run with the -h parameter then info is displayed that
 *  describes all of the options / parameters.
 */
public class SampleAsyncCallBack implements MqttCallback {

	int state = BEGIN;

	static final int BEGIN = 0;
	static final int CONNECTED = 1;
	static final int PUBLISHED = 2;
	static final int SUBSCRIBED = 3;
	static final int DISCONNECTED = 4;
	static final int FINISH = 5;
	static final int ERROR = 6;
	static final int DISCONNECT = 7;

	private static final int WALKING_WINDOW_SIZE = 250;

	/**
	 * The main entry point of the sample.
	 *
	 * This method handles parsing the arguments specified on the
	 * command-line before performing the specified action.
	 */
	public static void main(String[] args) {
		//readFromMQTT();
		try {
			readFromCSV();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	private static void readFromCSV() throws IOException {
		final String INPUT_1_PATH = "data_collect_2018_03_20_13_52_12.csv";
		final String INPUT_2_PATH = "data_collect_2018_03_20_14_08_54.csv";
		final String OUTPUT_PRESSURE_DIFF_PATH = "pressuresDifference";
		final String OUTPUT_PRESSURE_MOVING_AVERAGE = "pressureMA";
		final String OUTPUT_PRESSURE_DOUBLE_MOVING_AVERAGE = "pressure2MA";
		final String OUTPUT_PRESSURE_TRIPLE_MOVING_AVERAGE = "pressure3MA";
		final String OUTPUT_PRESSURE_MOVING_VARIANCES = "pressureMV";
		final String OUTPUT_WALKING = "walking";


		final int FILE_NUMBER = 2;
		final String OUTPUT_BASE = OUTPUT_WALKING;

		String inputPath = (FILE_NUMBER == 1)? INPUT_1_PATH : INPUT_2_PATH;

		String outputPath = (FILE_NUMBER == 1)? OUTPUT_BASE.concat("1.csv") : OUTPUT_BASE.concat("2.csv");

		loadDataFromCSV(inputPath);

		List<Double> result = new ArrayList<>();
		int stepDifference = 0;

		/*for(int i = WALKING_WINDOW_SIZE; i < accelerations.size() - WALKING_WINDOW_SIZE; i += WALKING_WINDOW_SIZE) { // the last seconds will be considered as idle
			stepDifference += stepCount(accelerations, i, WALKING_WINDOW_SIZE);

			System.out.println("Step difference : " + stepDifference);
			for(int j = 0; j < WALKING_WINDOW_SIZE; j++) {
				result.add((double) stepDifference);
			}

			/*if(stepDifference > 2) {
				for(int j = 0; j < WALKING_WINDOW_SIZE; j++) {
					result.add(1.0);
				}
			}
			else {
				for(int j = 0; j < WALKING_WINDOW_SIZE; j++) {
					result.add(0.0);
				}
			}

		}*/

		stepCount(accelerations, 0, accelerations.size());
		result = resultTest;

		//List<Double> result = leftMovingVariance(pressures, 6);

		PrintWriter writer = new PrintWriter(outputPath, "UTF-8");
		for(int i = 0; i < result.size(); i++){
			writer.println(result.get(i));
		}
		writer.close();
	}

	private static void loadDataFromCSV(String inputPath) throws IOException {
		List<String> list = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(inputPath));
		try {
			String line = reader.readLine();
			while (line != null) {
				list.add(line);
				line = reader.readLine();
			}
		} finally {
			reader.close();
		}

		for (String line : list) {
			String[] informations = line.split(",");
			switch (informations[1]) {
				case "a":
					double[] acceleration = new double[3];
					acceleration[0] = Double.parseDouble(informations[2]);
					acceleration[1] = Double.parseDouble(informations[3]);
					acceleration[2] = Double.parseDouble(informations[4]);
					accelerations.add(acceleration);
					break;

				case "b":
					pressures.add(Double.parseDouble(informations[2]));
					break;

				case "h":
					humidities.add(Double.parseDouble(informations[2]));
					break;

				case "l":
					lights.add(Double.parseDouble(informations[2]));
					break;

				case "t":
					temperatures.add(Double.parseDouble(informations[2]));
					break;

				default:
					System.out.println("Non parsed message");
					break;
			}
		}

		System.out.println(accelerations.size() + " acceleration samples");
		System.out.println(pressures.size() + " pressure samples");
		System.out.println(temperatures.size() + " temperature samples");
		System.out.println(lights.size() + " light samples");
		System.out.println(humidities.size() + " humidity samples");
	}

	private static List<Double> differences(List<Double> input) {
		List<Double> differences = new ArrayList<>();
		for (int i = 0; i+1 < input.size(); i++) {
			differences.add(input.get(i+1) - input.get(i));
		}
		differences.add(differences.get(differences.size()-1));
		return differences;
	}

	private static List<Double> leftMovingAverage(List<Double> input, int leftWindowSize) {
		List<Double> movingAverages = new ArrayList<>();
		for (int i = 0; i < leftWindowSize - 1; i++) {
			double sum = 0;
			for(int j = -i; j <= 0; j++) {
				sum += input.get(i+j);
			}
			movingAverages.add(sum/(i+1));
		}
		for (int i = leftWindowSize - 1; i < input.size(); i++) {
			double sum = 0;
			for(int j = -leftWindowSize + 1; j <= 0; j++) {
				sum += input.get(i+j);
			}
			movingAverages.add(sum/leftWindowSize);
		}
		return movingAverages;
	}

	private static List<Double> centeredMovingAverage(List<Double> input, int halfWindowSize) {
		List<Double> movingAveragePressures = new ArrayList<>();
		for (int i = 0; i < halfWindowSize; i++) {
			movingAveragePressures.add(0.0);
		}
		for (int i = halfWindowSize; i < input.size()-halfWindowSize; i++) {
			double sum = 0;
			for(int j = -halfWindowSize; j <= halfWindowSize; j++) {
				sum += input.get(i+j);
			}
			movingAveragePressures.add(sum/(halfWindowSize*2+1));
		}
		for (int i = 0; i < halfWindowSize; i++) {
			movingAveragePressures.set(i, movingAveragePressures.get(halfWindowSize));
		}
		for (int i = input.size()-halfWindowSize; i < input.size(); i++) {
			movingAveragePressures.add(movingAveragePressures.get(input.size()-halfWindowSize-1));
		}
		return movingAveragePressures;
	}

	private static List<Double> trend(List<Double> input, int leftWindowSize) {
		List<Double> trends = new ArrayList<>();

		return trends;
	}

	private static List<Double> leftMovingVariance(List<Double> input, int leftWindowSize) {
		List<Double> movingVariances = new ArrayList<>();
		List<Double> leftMovingAverages = leftMovingAverage(input, leftWindowSize);

		for(int i = 0; i < leftWindowSize - 1; i++) {
			movingVariances.add(0.0);
		}
		for(int i = leftWindowSize - 1; i < input.size(); i++) {
			double sum = 0;
			for(int j = -leftWindowSize + 1; j <= 0; j++) {
				sum += Math.pow(input.get(i+j) - leftMovingAverages.get(i+j), 2);
			}
			movingVariances.add(sum/leftWindowSize);
		}
		return movingVariances;
	}

	private static Integer stepCount(List<double[]> input, int startingIndex, int windowSize) {
		
		final int W = 12;       //moving variance is computed from i-W to i+W for index i
		final double T1 = 0.14;  //threshold value 1
		final double T2 = 0.13;  //threshold value 2

		List<double[]> accelerationsTruncated = new ArrayList<double[]>();
		List<Double> magnitudes = new ArrayList<Double>();
		List<Double> movingStandardDeviation = new ArrayList<Double>();
		List<Integer> phases = new ArrayList<Integer>();

		int step = 0;

		for (int j = startingIndex; j < startingIndex + windowSize; j++) {
			double[] emptyArr = {0.0, 0.0, 0.0};
			accelerationsTruncated.add(emptyArr);
			phases.add(2);
			double sum = 0.0;
			for(int i = 0; i < 3; i++) {
				accelerationsTruncated.get(accelerationsTruncated.size() - 1)[i] = input.get(j)[i];
				sum += Math.pow(accelerationsTruncated.get(accelerationsTruncated.size() - 1)[i], 2);
			}
			magnitudes.add(Math.sqrt(sum));
			movingStandardDeviation.add(0.0);
		}

		//We add a step each time the phase goes from swing to stance
		boolean hasBeenInSwingPhase = false;
		for(int i = W; i < accelerationsTruncated.size() - W; i++) {
			movingStandardDeviation.set(i, Math.sqrt(movingVariance(magnitudes, W, i)));
			resultTest.add(movingStandardDeviation.get(i));
			if(movingStandardDeviation.get(i) > T1) {
				phases.set(i, 1);   //swing phase
				hasBeenInSwingPhase = true;
			} else if (movingStandardDeviation.get(i) > T2) {
				phases.set(i, 0);   //in-between phase
			} else {
				if(hasBeenInSwingPhase) {
					hasBeenInSwingPhase = false;
					step++;
				}
				phases.set(i, 2);   //stance phase
			}
		}
		return step;
	}

	//this function calculates the moving variance at a specified index, with a specified number of samples before
	// and after the index (as defined in lecture 2 slide 25)
	private static double movingVariance(List<Double> accelerations, int numberOfSamples, int index) {
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

	private static void readFromMQTT() {
		// Default settings:
		boolean quietMode 	= false;
		String action 		= "subscribe";
		String topic 		= "delepine.matthieu@gmail.com";
		String message 		= "Message from Matth and Val from team 16 callback Paho MQTTv3 Java client sample";
		int qos 			= 0;	//2
		String broker 		= "ocean.comp.nus.edu.sg";	//m2m.eclipse.org
		int port 			= 1883;
		String clientId 	= null;
		String subTopic		= "#";
		String pubTopic 	= "Sample/Java/v3";
		boolean cleanSession = true;			// Non durable subscriptions
		boolean ssl = false;
		String password = "XOqN0GqlBXIl664x";
		String userName = "delepine.matthieu@gmail.com";

		// Validate the provided arguments
		if (!action.equals("publish") && !action.equals("subscribe")) {
			System.out.println("Invalid action: "+action);
			printHelp();
			return;
		}
		if (qos < 0 || qos > 2) {
			System.out.println("Invalid QoS: "+qos);
			printHelp();
			return;
		}
		if (topic.equals("")) {
			// Set the default topic according to the specified action
			if (action.equals("publish")) {
				topic = pubTopic;
			} else {
				topic = subTopic;
			}
		}

		String protocol = "tcp://";

		if (ssl) {
			protocol = "ssl://";
		}

		String url = protocol + broker + ":" + port;

		if (clientId == null || clientId.equals("")) {
			clientId = "SampleJavaV3_"+action;
		}

		// With a valid set of arguments, the real work of
		// driving the client API can begin
		try {
			// Create an instance of the Sample client wrapper
			SampleAsyncCallBack sampleClient = new SampleAsyncCallBack(url,clientId,cleanSession, quietMode,userName,password);

			// Perform the specified action
			if (action.equals("publish")) {
				sampleClient.publish(topic,qos,message.getBytes());
			} else if (action.equals("subscribe")) {
				sampleClient.subscribe(topic,qos);
			}
		} catch(MqttException me) {
			// Display full details of any exception that occurs
			System.out.println("reason "+me.getReasonCode());
			System.out.println("msg "+me.getMessage());
			System.out.println("loc "+me.getLocalizedMessage());
			System.out.println("cause "+me.getCause());
			System.out.println("excep "+me);
			me.printStackTrace();
		} catch (Throwable th) {
			System.out.println("Throwable caught "+th);
			th.printStackTrace();
		}
	}

	// Private instance variables
	MqttAsyncClient 	client;
	String 				brokerUrl;
	private boolean 			quietMode;
	private MqttConnectOptions 	conOpt;
	private boolean 			clean;
	Throwable 			ex = null;
	Object 				waiter = new Object();
	boolean 			donext = false;
	private String password;
	private String userName;
	private static List<double[]> accelerations = new ArrayList<>();
	private static List<Double> pressures = new ArrayList<>();
	private static List<Double> temperatures = new ArrayList<>();
	private static List<Double> lights = new ArrayList<>();
	private static List<Double> humidities = new ArrayList<>();
	private static List<Integer> stepCount = new ArrayList<>();
	private static List<Double> resultTest = new ArrayList<>();


	/**
	 * Constructs an instance of the sample client wrapper
	 * @param brokerUrl the url to connect to
	 * @param clientId the client id to connect with
	 * @param cleanSession clear state at end of connection or not (durable or non-durable subscriptions)
	 * @param quietMode whether debug should be printed to standard out
	 * @param userName the username to connect with
	 * @param password the password for the user
	 * @throws MqttException
	 */
    public SampleAsyncCallBack(String brokerUrl, String clientId, boolean cleanSession,
    		boolean quietMode, String userName, String password) throws MqttException {
    	this.brokerUrl = brokerUrl;
    	this.quietMode = quietMode;
    	this.clean 	   = cleanSession;
      this.password = password;
      this.userName = userName;

    	//This sample stores in a temporary directory... where messages temporarily
    	// stored until the message has been delivered to the server.
    	//..a real application ought to store them somewhere
    	// where they are not likely to get deleted or tampered with
    	String tmpDir = System.getProperty("java.io.tmpdir");
    	MqttDefaultFilePersistence dataStore = new MqttDefaultFilePersistence(tmpDir);

    	try {
    		// Construct the object that contains connection parameters
    		// such as cleanSession and LWT
	    	conOpt = new MqttConnectOptions();
	    	conOpt.setCleanSession(clean);
	    	if(password != null ) {
          conOpt.setPassword(this.password.toCharArray());
        }
        if(userName != null) {
          conOpt.setUserName(this.userName);
        }

    		// Construct the MqttClient instance
			client = new MqttAsyncClient(this.brokerUrl,clientId, dataStore);

			// Set this wrapper as the callback handler
	    	client.setCallback(this);

		} catch (MqttException e) {
			e.printStackTrace();
			log("Unable to set up client: "+e.toString());
			System.exit(1);
		}
    }

    /**
     * Publish / send a message to an MQTT server
     * @param topicName the name of the topic to publish to
     * @param qos the quality of service to delivery the message at (0,1,2)
     * @param payload the set of bytes to send to the MQTT server
     * @throws MqttException
     */
    public void publish(String topicName, int qos, byte[] payload) throws Throwable {
    	// Use a state machine to decide which step to do next. State change occurs
    	// when a notification is received that an MQTT action has completed
    	while (state != FINISH) {
    		switch (state) {
    			case BEGIN:
    				// Connect using a non-blocking connect
    		    	MqttConnector con = new MqttConnector();
    		    	con.doConnect();
    				break;
    			case CONNECTED:
    				// Publish using a non-blocking publisher
    				Publisher pub = new Publisher();
    				pub.doPublish(topicName, qos, payload);
    				break;
    			case PUBLISHED:
    				state = DISCONNECT;
    				donext = true;
    				break;
    			case DISCONNECT:
    				Disconnector disc = new Disconnector();
    				disc.doDisconnect();
    				break;
    			case ERROR:
    				throw ex;
    			case DISCONNECTED:
    				state = FINISH;
    				donext = true;
    				break;
    		}

//    		if (state != FINISH) {
    			// Wait until notified about a state change and then perform next action
    			waitForStateChange(10000);
//    		}
    	}
    }

    /**
     * Wait for a maximum amount of time for a state change event to occur
     * @param maxTTW  maximum time to wait in milliseconds
     * @throws MqttException
     */
	private void waitForStateChange(int maxTTW ) throws MqttException {
		synchronized (waiter) {
    		if (!donext ) {
    			try {
					waiter.wait(maxTTW);
				} catch (InterruptedException e) {
					log("timed out");
					e.printStackTrace();
				}

				if (ex != null) {
					throw (MqttException)ex;
				}
    		}
    		donext = false;
    	}
	}

    /**
     * Subscribe to a topic on an MQTT server
     * Once subscribed this method waits for the messages to arrive from the server
     * that match the subscription. It continues listening for messages until the enter key is
     * pressed.
     * @param topicName to subscribe to (can be wild carded)
     * @param qos the maximum quality of service to receive messages at for this subscription
     * @throws MqttException
     */
    public void subscribe(String topicName, int qos) throws Throwable {
    	// Use a state machine to decide which step to do next. State change occurs
    	// when a notification is received that an MQTT action has completed
    	while (state != FINISH) {
    		switch (state) {
    			case BEGIN:
    				// Connect using a non-blocking connect
    		    	MqttConnector con = new MqttConnector();
    		    	con.doConnect();
    				break;
    			case CONNECTED:
    				// Subscribe using a non-blocking subscribe
    				Subscriber sub = new Subscriber();
    				sub.doSubscribe(topicName, qos);
    				break;
    			case SUBSCRIBED:
    		    	// Block until Enter is pressed allowing messages to arrive
    		    	log("Press <Enter> to exit");
    				try {
    					System.in.read();
    				} catch (IOException e) {
    					//If we can't read we'll just exit
    				}
    				state = DISCONNECT;
    				donext = true;
    				break;
    			case DISCONNECT:
    				System.out.println(accelerations.size() + " acceleration samples");
					System.out.println(pressures.size() + " pressure samples");
					System.out.println(temperatures.size() + " temperature samples");
					System.out.println(lights.size() + " light samples");
					System.out.println(humidities.size() + " humidity samples");

    				Disconnector disc = new Disconnector();
    				disc.doDisconnect();
    				break;
    			case ERROR:
    				throw ex;
    			case DISCONNECTED:
    				state = FINISH;
    				donext = true;
    				break;
    		}

//    		if (state != FINISH && state != DISCONNECT) {
    			waitForStateChange(10000);
    		}
//    	}
    }

    /**
     * Utility method to handle logging. If 'quietMode' is set, this method does nothing
     * @param message the message to log
     */
    void log(String message) {
    	if (!quietMode) {
    		System.out.println(message);
    	}
    }

	/****************************************************************/
	/* Methods to implement the MqttCallback interface              */
	/****************************************************************/

    /**
     * @see MqttCallback#connectionLost(Throwable)
     */
	public void connectionLost(Throwable cause) {
		// Called when the connection to the server has been lost.
		// An application may choose to implement reconnection
		// logic at this point. This sample simply exits.
		log("Connection to " + brokerUrl + " lost!" + cause);
		System.exit(1);
	}

    /**
     * @see MqttCallback#deliveryComplete(IMqttDeliveryToken)
     */
	public void deliveryComplete(IMqttDeliveryToken token) {
		// Called when a message has been delivered to the
		// server. The token passed in here is the same one
		// that was returned from the original call to publish.
		// This allows applications to perform asynchronous
		// delivery without blocking until delivery completes.
		//
		// This sample demonstrates asynchronous deliver, registering
		// a callback to be notified on each call to publish.
		//
		// The deliveryComplete method will also be called if
		// the callback is set on the client
		//
		// note that token.getTopics() returns an array so we convert to a string
		// before printing it on the console
		log("Delivery complete callback: Publish Completed "+Arrays.toString(token.getTopics()));
	}

    /**
     * @see MqttCallback#messageArrived(String, MqttMessage)
     */
	public void messageArrived(String topic, MqttMessage message) throws MqttException {
		// Called when a message arrives from the server that matches any
		// subscription made by the client
		String time = new Timestamp(System.currentTimeMillis()).toString();
		/*System.out.println("Time:\t" +time +
                           "  Topic:\t" + topic +
                           "  Message:\t" + new String(message.getPayload()) +
                           "  QoS:\t" + message.getQos());*/
		readPayload(new String(message.getPayload()));
	}

	/****************************************************************/
	/* End of MqttCallback methods                                  */
	/****************************************************************/
    static void printHelp() {
      System.out.println(
          "Syntax:\n\n" +
              "    SampleAsyncCallBack [-h] [-a publish|subscribe] [-t <topic>] [-m <message text>]\n" +
              "            [-s 0|1|2] -b <hostname|IP address>] [-p <brokerport>] [-i <clientID>]\n\n" +
              "    -h  Print this help text and quit\n" +
              "    -q  Quiet mode (default is false)\n" +
              "    -a  Perform the relevant action (default is publish)\n" +
              "    -t  Publish/subscribe to <topic> instead of the default\n" +
              "            (publish: \"Sample/Java/v3\", subscribe: \"Sample/#\")\n" +
              "    -m  Use <message text> instead of the default\n" +
              "            (\"Message from MQTTv3 Java client\")\n" +
              "    -s  Use this QoS instead of the default (2)\n" +
              "    -b  Use this name/IP address instead of the default (m2m.eclipse.org)\n" +
              "    -p  Use this port instead of the default (1883)\n\n" +
              "    -i  Use this client ID instead of SampleJavaV3_<action>\n" +
              "    -c  Connect to the server with a clean session (default is false)\n" +
              "     \n\n Security Options \n" +
              "     -u Username \n" +
              "     -z Password \n" +
              "     \n\n SSL Options \n" +
              "    -v  SSL enabled; true - (default is false) " +
              "    -k  Use this JKS format key store to verify the client\n" +
              "    -w  Passpharse to verify certificates in the keys store\n" +
              "    -r  Use this JKS format keystore to verify the server\n" +
              " If javax.net.ssl properties have been set only the -v flag needs to be set\n" +
              "Delimit strings containing spaces with \"\"\n\n" +
              "Publishers transmit a single message then disconnect from the server.\n" +
              "Subscribers remain connected to the server and receive appropriate\n" +
              "messages until <enter> is pressed.\n\n"
          );
    }

    private void readPayload(String payload) {
    	//reads the payload and inserts the data into the arrays
		//todo : handle the case of packet loss (corrupted payload string)
		String[] parts = payload.split("\"");
		String[] subparts = parts[7].split(",");
		for (int i = 0; i < subparts.length; i++) {
			System.out.print(subparts[i] + "; ");
		}
		System.out.println();
		switch (subparts[2]) {
			case "a":
				double[] acceleration = new double[3];
				acceleration[0] = Double.parseDouble(subparts[3]);
				acceleration[1] = Double.parseDouble(subparts[4]);
				acceleration[2] = Double.parseDouble(subparts[5]);
				accelerations.add(acceleration);
				break;

			case "b":
				pressures.add(Double.parseDouble(subparts[3]));
				break;

			case "h":
				humidities.add(Double.parseDouble(subparts[3]));
				break;

			case "l":
				lights.add(Double.parseDouble(subparts[3]));
				break;

			case "t":
				temperatures.add(Double.parseDouble(subparts[3]));
				break;

			default:
				System.out.println("Non parsed message");
				break;
		}
	}



	/**
	 * Connect in a non-blocking way and then sit back and wait to be
	 * notified that the action has completed.
	 */
    public class MqttConnector {

		public MqttConnector() {
		}

		public void doConnect() {
	    	// Connect to the server
			// Get a token and setup an asynchronous listener on the token which
			// will be notified once the connect completes
	    	log("Connecting to "+brokerUrl + " with client ID "+client.getClientId());

	    	IMqttActionListener conListener = new IMqttActionListener() {
				public void onSuccess(IMqttToken asyncActionToken) {
			    	log("Connected");
			    	state = CONNECTED;
			    	carryOn();
				}

				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					ex = exception;
					state = ERROR;
					log ("connect failed" +exception);
					carryOn();
				}

				public void carryOn() {
			    	synchronized (waiter) {
			    		donext=true;
			    		waiter.notifyAll();
			    	}
				}
			};

	    	try {
	    		// Connect using a non-blocking connect
	    		client.connect(conOpt,"Connect sample context", conListener);
			} catch (MqttException e) {
				// If though it is a non-blocking connect an exception can be
				// thrown if validation of parms fails or other checks such
				// as already connected fail.
				state = ERROR;
				donext = true;
				ex = e;
			}
		}
	}

	/**
	 * Publish in a non-blocking way and then sit back and wait to be
	 * notified that the action has completed.
	 */
	public class Publisher {
		public void doPublish(String topicName, int qos, byte[] payload) {
		 	// Send / publish a message to the server
			// Get a token and setup an asynchronous listener on the token which
			// will be notified once the message has been delivered
	   		MqttMessage message = new MqttMessage(payload);
	    	message.setQos(qos);


	    	String time = new Timestamp(System.currentTimeMillis()).toString();
	    	log("Publishing at: "+time+ " to topic \""+topicName+"\" qos "+qos);

	    	// Setup a listener object to be notified when the publish completes.
	    	//
	    	IMqttActionListener pubListener = new IMqttActionListener() {
				public void onSuccess(IMqttToken asyncActionToken) {
			    	log("Publish Completed");
			    	state = PUBLISHED;
			    	carryOn();
				}

				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					ex = exception;
					state = ERROR;
					log ("Publish failed" +exception);
					carryOn();
				}

				public void carryOn() {
			    	synchronized (waiter) {
			    		donext=true;
			    		waiter.notifyAll();
			    	}
				}
			};

	    	try {
		    	// Publish the message
	    		client.publish(topicName, message, "Pub sample context", pubListener);
	    	} catch (MqttException e) {
				state = ERROR;
				donext = true;
				ex = e;
			}
		}
	}

	/**
	 * Subscribe in a non-blocking way and then sit back and wait to be
	 * notified that the action has completed.
	 */
	public class Subscriber {
		public void doSubscribe(String topicName, int qos) {
		 	// Make a subscription
			// Get a token and setup an asynchronous listener on the token which
			// will be notified once the subscription is in place.
	    	log("Subscribing to topic \""+topicName+"\" qos "+qos);

	    	IMqttActionListener subListener = new IMqttActionListener() {
				public void onSuccess(IMqttToken asyncActionToken) {
			    	log("Subscribe Completed");
			    	state = SUBSCRIBED;
			    	carryOn();
				}

				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					ex = exception;
					state = ERROR;
					log ("Subscribe failed" +exception);
					carryOn();
				}

				public void carryOn() {
			    	synchronized (waiter) {
			    		donext=true;
			    		waiter.notifyAll();
			    	}
				}
			};

	    	try {
	    		client.subscribe(topicName, qos, "Subscribe sample context", subListener);
	    	} catch (MqttException e) {
				state = ERROR;
				donext = true;
				ex = e;
			}
		}
	}

	/**
	 * Disconnect in a non-blocking way and then sit back and wait to be
	 * notified that the action has completed.
	 */
	public class Disconnector {
		public void doDisconnect() {
	    	// Disconnect the client
	    	log("Disconnecting");

	    	IMqttActionListener discListener = new IMqttActionListener() {
				public void onSuccess(IMqttToken asyncActionToken) {
			    	log("Disconnect Completed");
			    	state = DISCONNECTED;
			    	carryOn();
				}

				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					ex = exception;
					state = ERROR;
					log ("Disconnect failed" +exception);
					carryOn();
				}
				public void carryOn() {
			    	synchronized (waiter) {
			    		donext=true;
			    		waiter.notifyAll();
			    	}
				}
			};

	    	try {
	    		client.disconnect("Disconnect sample context", discListener);
	    	} catch (MqttException e) {
				state = ERROR;
				donext = true;
				ex = e;
			}
		}
	}
}
