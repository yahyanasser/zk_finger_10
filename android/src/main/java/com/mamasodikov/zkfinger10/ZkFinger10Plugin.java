package com.mamasodikov.zkfinger10;

import android.app.Activity;
import android.content.Context;
import android.graphics.Bitmap;

import com.mamasodikov.zkfinger10.util.FingerListener;
import com.mamasodikov.zkfinger10.util.FingerStatus;
import com.mamasodikov.zkfinger10.util.FingerStatusType;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.flutter.embedding.engine.plugins.FlutterPlugin;
import io.flutter.embedding.engine.plugins.activity.ActivityAware;
import io.flutter.embedding.engine.plugins.activity.ActivityPluginBinding;
import io.flutter.plugin.common.BinaryMessenger;
import io.flutter.plugin.common.EventChannel;
import io.flutter.plugin.common.MethodCall;
import io.flutter.plugin.common.MethodChannel;
import io.flutter.plugin.common.MethodChannel.MethodCallHandler;
import io.flutter.plugin.common.MethodChannel.Result;
import io.reactivex.Observer;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;

/**
 * ZkFingerPlugin
 */
public class ZkFinger10Plugin implements FlutterPlugin, ActivityAware, MethodChannel.MethodCallHandler, FingerListener {

    private static final String METHOD_FINGER_OPEN_CONNECTION =
            "openConnection";
    private static final String METHOD_FINGER_CLOSE_CONNECTION =
            "closeConnection";
    private static final String METHOD_FINGER_START_LISTEN =
            "startListen";
    private static final String METHOD_FINGER_STOP_LISTEN =
            "stopListen";
    private static final String METHOD_FINGER_IDENTIFY =
            "identify";
    private static final String METHOD_FINGER_VERIFY =
            "verify";
    private static final String METHOD_FINGER_REGISTER =
            "register";
    private static final String METHOD_FINGER_CLEAR =
            "clear";
    private static final String METHOD_CLEAR_AND_LOAD =
            "clearAndLoad";
    private static final String METHOD_FINGER_DELETE =
            "delete";
    private static final String METHOD_ON_DESTROY =
            "onDestroy";

    private static final String CHANNEL_FINGER_STATUS_CHANGE = "com.mamasodikov.zkfinger10/status_change";
    private static final String CHANNEL_FINGER_IMAGE = "com.mamasodikov.zkfinger10/finger_image";
    private static PublishSubject<FingerStatus> fingerStatusSubject = PublishSubject.create();
    private static PublishSubject<byte[]> fingerImageSubject = PublishSubject.create();
    private MethodChannel channel;
    private Activity activity;
    private Context applicationContext;
    private ZKFingerPrintHelper zkFingerPrintHelper;
    private Result result;

    // FlutterPlugin lifecycle
    @Override
    public void onAttachedToEngine(FlutterPlugin.FlutterPluginBinding binding) {
        applicationContext = binding.getApplicationContext();
        BinaryMessenger messenger = binding.getBinaryMessenger();
        channel = new MethodChannel(messenger, "zkfinger");
        channel.setMethodCallHandler(this);

        // Initialize event channels using the new messenger.
        initFingerStatusChangeListener(messenger);
        initFingerImageListener(messenger);
    }

    @Override
    public void onDetachedFromEngine(FlutterPlugin.FlutterPluginBinding binding) {
        channel.setMethodCallHandler(null);
    }

    // ActivityAware lifecycle
    @Override
    public void onAttachedToActivity(ActivityPluginBinding binding) {
        activity = binding.getActivity();
    }

    @Override
    public void onDetachedFromActivityForConfigChanges() {
        activity = null;
    }

    @Override
    public void onReattachedToActivityForConfigChanges(ActivityPluginBinding binding) {
        activity = binding.getActivity();
    }

    @Override
    public void onDetachedFromActivity() {
        activity = null;
    }

    // Update init methods to use BinaryMessenger
    private static void initFingerStatusChangeListener(BinaryMessenger messenger) {
        EventChannel statusChangeEventChannel = new EventChannel(messenger, CHANNEL_FINGER_STATUS_CHANGE);
        statusChangeEventChannel.setStreamHandler(new EventChannel.StreamHandler() {
            @Override
            public void onListen(Object o, final EventChannel.EventSink eventSink) {
                fingerStatusSubject.subscribeOn(Schedulers.newThread())
                        .observeOn(AndroidSchedulers.mainThread())
                        .subscribe(new Observer<FingerStatus>() {
                            @Override
                            public void onSubscribe(Disposable d) { }
                            @Override
                            public void onNext(FingerStatus status) {
                                HashMap<String, Object> statusMap = new HashMap<>();
                                statusMap.put("id", status.getId());
                                statusMap.put("message", status.getMessage());
                                statusMap.put("data", status.getData());
                                statusMap.put("fingerStatus", status.getFingerStatusType().ordinal());
                                eventSink.success(statusMap);
                            }
                            @Override
                            public void onError(Throwable e) { }
                            @Override
                            public void onComplete() { }
                        });
            }
            @Override
            public void onCancel(Object o) { }
        });
    }

    private static void initFingerImageListener(BinaryMessenger messenger) {
        EventChannel imageEventChannel = new EventChannel(messenger, CHANNEL_FINGER_IMAGE);
        imageEventChannel.setStreamHandler(new EventChannel.StreamHandler() {
            @Override
            public void onListen(Object o, final EventChannel.EventSink eventSink) {
                fingerImageSubject.subscribeOn(Schedulers.newThread())
                        .observeOn(AndroidSchedulers.mainThread())
                        .subscribe(new Observer<byte[]>() {
                            @Override
                            public void onSubscribe(Disposable d) { }
                            @Override
                            public void onNext(byte[] imageBytes) {
                                eventSink.success(imageBytes);
                            }
                            @Override
                            public void onError(Throwable e) { }
                            @Override
                            public void onComplete() { }
                        });
            }
            @Override
            public void onCancel(Object o) { }
        });
    }

    @Override
    public void onMethodCall(MethodCall call, MethodChannel.Result result) {
        this.result = result;
        if (zkFingerPrintHelper == null)
            zkFingerPrintHelper = new ZKFingerPrintHelper(activity, applicationContext, this);

        switch (call.method) {
            case "getPlatformVersion":
                result.success("Android " + android.os.Build.VERSION.RELEASE);
                break;
            case METHOD_FINGER_OPEN_CONNECTION:
                openConnection();
                break;
            case METHOD_FINGER_CLOSE_CONNECTION:
                closeConnection();
                break;
            case METHOD_FINGER_START_LISTEN:
                startFingerListen(getUserId(call));
                break;
            case METHOD_FINGER_STOP_LISTEN:
                stopFingerListen();
                break;
            case METHOD_FINGER_IDENTIFY:
                identifyFinger(getUserId(call));
                break;
            case METHOD_FINGER_VERIFY:
                verifyFinger(getUserFingers(call).get("finger1"), getUserFingers(call).get("finger2"));
                break;
            case METHOD_FINGER_REGISTER:
                registerFinger(getUserId(call));
                break;
            case METHOD_FINGER_CLEAR:
                clearFingers();
                break;
            case METHOD_CLEAR_AND_LOAD:
                clearAndLoad(getFingersMap(call));
                break;
            case METHOD_FINGER_DELETE:
                deleteFinger(getUserId(call));
                break;
            case METHOD_ON_DESTROY:
                onDestroy();
                break;
            default:
                result.notImplemented();
        }
    }

    private String getUserId(MethodCall call) {
        return call.argument("id");
    }

    Map<String, String> getUserFingers(MethodCall call) {
        Map<String, Object> fingers = (Map<String, Object>) call.arguments;
        String finger1 = (String) fingers.get("finger1");
        String finger2 = (String) fingers.get("finger2");
        Map<String, String> fingersMap = new HashMap<>();
        fingersMap.put("finger1", finger1);
        fingersMap.put("finger2", finger2);
        return fingersMap;
    }

    private Map<String, String> getFingersMap(MethodCall call) {
        return call.argument("fingers");
    }

    private boolean isLogEnabled(MethodCall call) {
        return call.argument("isLogEnabled");
    }

    private String getFingerData(MethodCall call) {
        return call.argument("data");
    }

    private void openConnection() {
        zkFingerPrintHelper.openDevice();
        result.success(true);
    }

    private void closeConnection() {
        zkFingerPrintHelper.closeDevice();
        result.success(true);
    }


    private void startFingerListen(String userId) {
        zkFingerPrintHelper.startFingerSensor(userId);
        result.success(true);
    }

    private void stopFingerListen() {
        zkFingerPrintHelper.stopFingerSensor();
        result.success(true);
    }

    private void registerFinger(String userId) {
        zkFingerPrintHelper.registerFinger(userId);
        result.success(true);
    }

    private void identifyFinger(String userId) {
        zkFingerPrintHelper.identifyFinger(userId);
        result.success(true);
    }

    private void verifyFinger(String template1, String template2) {
        zkFingerPrintHelper.verify(template1, template2);
        result.success(true);
    }

    private void clearAndLoad(Map<String, String> vUserList) {
        zkFingerPrintHelper.clearAndLoad(vUserList);
        result.success(true);
    }


    private void clearFingers() {
        zkFingerPrintHelper.clear();
        result.success(true);
    }

    private void deleteFinger(String userId) {
        zkFingerPrintHelper.deleteFinger(userId);
        result.success(true);
    }

    private void onDestroy() {
        zkFingerPrintHelper.onDestroy();
        result.success(true);
    }


    @Override
    public void onStatusChange(String message, FingerStatusType fingerStatusType, String id, String data) {
        fingerStatusSubject.onNext(new FingerStatus(message, fingerStatusType, id, data));
    }

    @Override
    public void onCaptureFinger(Bitmap fingerBitmap) {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        fingerBitmap.compress(Bitmap.CompressFormat.PNG, 100, stream);
        byte[] byteArray = stream.toByteArray();
        fingerBitmap.recycle();

        fingerImageSubject.onNext(byteArray);
    }

}


