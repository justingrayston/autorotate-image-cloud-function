const {Storage} = require('@google-cloud/storage');
const {PubSub} = require('@google-cloud/pubsub');
const fs = require('fs');
const path = require('path');
const os = require('os');
const ja = require('jpeg-autorotate');

const gcs = new Storage();
const pubsubClient = new PubSub();

/**
 * return The temporary destination.
 * @param {string} fileName
 */
function getTempPath(fileName) {
  fileName = path.basename(fileName);
  return path.join(os.tmpdir(), fileName);
}

/**
 * Save the image from a bucket to local for
 * processing.
 * @param {gcsObject} gcsObject
 */
function saveImageToTmp(gcsObject) {
  const bucket = gcs.bucket(gcsObject.bucket);
  console.log(`Download file from bucket`);
  return bucket.file(gcsObject.name).download({
    destination: getTempPath(gcsObject.name)
  });
}

function removeTmpImage(tempPath) {
  return fs.unlinkSync(tempPath);
}

function autorotate(tmpPath) {
  console.log(`Temp path ${tmpPath}`);
  return new Promise((resolve, reject) => {
    ja.rotate(tmpPath, {}, (error, buffer, orientation, dimensions) => {
      console.log(orientation);
      if (error) {
        const msg = `An error occered when rotating file ${tmpPath}: ${error.message}`
        console.error(msg);
        reject(error);
        return;
      }
      console.log('Orientation was: ' + orientation);
      console.log('Height after rotation: ' + dimensions.height);
      console.log('Width after rotation: ' + dimensions.width);
      fs.writeFile(tmpPath, buffer, (err) => {
        if (!err) {
          resolve();
        } else {
          reject(err);
        }
      });
    });
  });
}

function uploadRotatedImage(gcsObject, tmpPath) {
  const bucket = gcs.bucket(gcsObject.bucket);
  const metadata = gcsObject.metadata || {};
  metadata['rotated'] = true;
  return bucket.upload(tmpPath, {
    destination: gcsObject.name,
    metadata: metadata,
    resumable: false
  });
}

function isJpeg(contentType) {
  if (contentType.startsWith('image/jpg') || contentType.startsWith('image/jpeg')) {
    return true;
  }
  return false;
}

function notifyViaPubSub(gcsObject) {
  const topic = process.env['IMAGE_ORIENTATION_NOTIFY_TOPIC'];
  const dataBuffer = Buffer.from(JSON.stringify(gcsObject));
  return pubsubClient.topic(topic).publish(dataBuffer);
}

exports.autoRotateImage = (event) => {
  // First load the image to store locally while we process.
  const gcsObject = event;

  // Don't run if already rotated.
  if (gcsObject.metageneration > 1 ) {
    console.log('already rotated');
    return;
  }

  // only process jpegs
  if (!isJpeg(gcsObject.contentType)) {
    console.log(`Not a jpeg, exit. ${gcsObject.contentType}`);
    return;
  }

  const tempPath = getTempPath(gcsObject.name);

  return saveImageToTmp(gcsObject)
    .then((downloaded) => {
      console.log(downloaded);
      console.log('downloaded, now rotate');
      return autorotate(tempPath);
    })
    .then(() => {
      console.log('upload to gcs');
      return uploadRotatedImage(gcsObject, tempPath);
    })
    .then(() => {
      return notifyViaPubSub(gcsObject);
    })
    .then(() => {
      console.log('remove tmp file');
      return removeTmpImage(tempPath);
    })
    .catch((error) => {
      console.log(`Error was caught ${error}`);
      console.error(error);
    });
}
