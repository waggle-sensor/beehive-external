<?php

// Endpoint: https://web.lcrc.anl.gov/public/waggle/private/training_data/metrics.php
// Description: Serves training size metrics.
// 

$filesync = [
    "001e06117b41,image_bottom" => 1,
    "001e06117b45,image_bottom" => 1,
    "001e061181b9,image_bottom" => 1,
    "001e061182a2,image_bottom" => 1,
    "001e061182a3,image_bottom" => 1,
    "001e061182ae,image_bottom" => 1,
    "001e061182bb,image_bottom" => 1,
    "001e061182c0,image_bottom" => 1,
    "001e061182c1,image_bottom" => 1,
    "001e061182e8,image_bottom" => 1,
    "001e06118366,image_bottom" => 1,
    "001e061183bf,image_bottom" => 1,
    "001e061183ec,image_bottom" => 1,
    "001e061183f3,image_bottom" => 1,
    "001e061183f5,image_bottom" => 1,
    "001e061184a3,image_bottom" => 1,
    "001e06118501,image_bottom" => 1,
    "001e0611863a,image_bottom" => 1,
];

$nodes = [
    "001e06117b41" => 1,
    "001e06117b45" => 1,
    "001e061181b9" => 1,
    "001e061182a2" => 1,
    "001e061182a3" => 1,
    "001e061182ae" => 1,
    "001e061182bb" => 1,
    "001e061182c0" => 1,
    "001e061182c1" => 1,
    "001e061182e8" => 1,
    "001e06118366" => 1,
    "001e061183bf" => 1,
    "001e061183ec" => 1,
    "001e061183f3" => 1,
    "001e061183f5" => 1,
    "001e061184a3" => 1,
    "001e06118501" => 1,
    "001e0611863a" => 1,
];

printf("# HELP waggle_training_data_total Number of items in training data.\n");
printf("# TYPE waggle_training_data_total gauge\n");

function printResourceMetrics($basedir1, $basedir2, $subdir, $resource, $class, $ext) {
    $totals = [];

    // match directories like:
    // aot_audio_and_images/image_bottom/001e061182bb/2020-04-07T02:40:22+00:00.jpg
    // ----- basedir -------------------/---nodeID---/*.jpg
    try {
        foreach (new FilesystemIterator($basedir1) as $dir) {
            if (!$dir->isDir()) {
                continue;
            }

            $nodeID = $dir->getBasename();
            $total = (new GlobIterator($dir . "/" . $ext))->count();

            if (!isset($totals[$nodeID])) {
                $totals[$nodeID] = 0;
            }

            $totals[$nodeID] += $total;
        }
    } catch (Exception $e) {
    }

    // match directories like:
    // aot_audio_and_images/good/001e061182bb/image/bottom/2020/04/07/2020-04-07T02:40:22+00:00.jpg
    // ----- basedir -----------/---nodeID---/---subdir---/YYYY/MM/DD/*.jpg
    try {
        foreach (new FilesystemIterator($basedir2) as $dir) {
            if (!$dir->isDir()) {
                continue;
            }

            $nodeID = $dir->getBasename();
            $total = (new GlobIterator($dir . "/" . $subdir . "/*/*/*/" . $ext))->count();

            if (!isset($totals[$nodeID])) {
                $totals[$nodeID] = 0;
            }

            $totals[$nodeID] += $total;
        }
    } catch (Exception $e) {
    }

    foreach ($totals as $nodeID => $total) {
        printf("waggle_training_data_total{node_id=\"%s\",resource=\"%s\",class=\"%s\"} %d\n", $nodeID, $resource, $class, $total);
        // keep track of nodeID
        $nodes[$nodeID] = true;
    }
}

printResourceMetrics("aot_audio_and_images/image_bottom", "aot_audio_and_images/good", "image/bottom", "image_bottom", "good", "*.jpg");
printResourceMetrics("aot_audio_and_images/bad_image_bottom", "aot_audio_and_images/bad", "image/bottom", "image_bottom", "bad", "*.jpg");
printResourceMetrics("aot_audio_and_images/error_image_bottom", "aot_audio_and_images/error", "image/bottom", "image_bottom", "error", "*.jpg");

printResourceMetrics("aot_audio_and_images/image_top", "aot_audio_and_images/good", "image/top", "image_top", "good", "*.jpg");
printResourceMetrics("aot_audio_and_images/bad_image_top", "aot_audio_and_images/bad", "image/top", "image_top", "bad", "*.jpg");
printResourceMetrics("aot_audio_and_images/error_image_top", "aot_audio_and_images/error", "image/top", "image_top", "error", "*.jpg");

printResourceMetrics("aot_audio_and_images/audio_microphone", "aot_audio_and_images/good", "audio/microphone", "audio_microphone", "good", "*.mp3");
printResourceMetrics("aot_audio_and_images/bad_audio_microphone", "aot_audio_and_images/bad", "audio/microphone", "audio_microphone", "bad", "*.mp3");
printResourceMetrics("aot_audio_and_images/error_audio_microphone", "aot_audio_and_images/error", "audio/microphone", "audio_microphone", "error", "*.mp3");

printResourceMetrics("aot_audio_and_images/video_bottom", "aot_audio_and_images/good", "video/bottom", "video_bottom", "good", "*.mp4");
printResourceMetrics("aot_audio_and_images/bad_video_bottom", "aot_audio_and_images/bad", "video/bottom", "video_bottom", "bad", "*.mp4");
printResourceMetrics("aot_audio_and_images/error_video_bottom", "aot_audio_and_images/error", "video/bottom", "video_bottom", "error", "*.mp4");

printResourceMetrics("aot_audio_and_images/video_top", "aot_audio_and_images/good", "video/top", "video_top", "good", "*.mp4");
printResourceMetrics("aot_audio_and_images/bad_video_top", "aot_audio_and_images/bad", "video/top", "video_top", "bad", "*.mp4");
printResourceMetrics("aot_audio_and_images/error_video_top", "aot_audio_and_images/error", "video/top", "video_top", "error", "*.mp4");

foreach (["image_bottom", "image_top", "audio_microphone", "video_bottom", "video_top"] as $resource) {
    foreach ($nodes as $nodeID => $_) {
        $k = $nodeID . "," . $resource;

        if (isset($filesync[$k])) {
            $is_enabled = $filesync[$k];
        } else {
            $is_enabled = 0;
        }

        printf("waggle_training_data_is_enabled{node_id=\"%s\",resource=\"%s\"} %d\n", $nodeID, $resource, $is_enabled);
    }
}
?>
