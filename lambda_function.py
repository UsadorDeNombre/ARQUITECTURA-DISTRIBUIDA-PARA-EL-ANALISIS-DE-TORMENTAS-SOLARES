import boto3
import os
import sys
import uuid
from urllib.parse import unquote_plus
import numpy as np
from PIL import Image, ImageDraw
import cv2
import json

FIRST_FILTER_WHITE_PIXELS_PERCENTAGE_THRESHOLD = 5.05
SECOND_FILTER_WHITE_PIXELS_PERCENTAGE_THRESHOLD = 0.00939 

SECOND_FILTER_MEDIUM_SOLAR_FLARE_THRESHOLD = 0.04
SECOND_FILTER_STRONG_SOLAR_FLARE_THRESHOLD = 0.06

PIXEL_LIGHT_INTENSITY_THRESHOLD = 245
DEFAULT_SUN_CIRCLE_PIXEL_RADIUS = 202

CIRCLE_PIXEL_RADIUS_CORRECTION = 3
MASK_SIZE = (512, 512)

s3_client = boto3.client('s3')

def circle_detection(image_path):
    cv2_image = cv2.imread(image_path)
    cv2_grey_image = cv2.cvtColor(cv2_image, cv2.COLOR_BGR2GRAY)
    cv2_grey_image = cv2.medianBlur(cv2_grey_image, 5)
    circle_detected = cv2.HoughCircles(cv2_grey_image,
                        cv2.HOUGH_GRADIENT,
                        dp=1.2,
                        minDist=1000,
                        param1=70,
                        param2=25,
                        minRadius=150,
                        maxRadius=250)
    if circle_detected is not None:
        circle_detected = np.round(circle_detected[0, :]).astype("int")
        return circle_detected[0][2] - CIRCLE_PIXEL_RADIUS_CORRECTION
    return DEFAULT_SUN_CIRCLE_PIXEL_RADIUS

def create_mask_image(sun_circle_pixel_radius):
    mask_image = Image.new("RGBA", MASK_SIZE, (0,0,0,0))
    mask_circle = ImageDraw.Draw(mask_image)
    mask_circle.rectangle([6, 490, 48, 504], fill="black")
    mask_circle.rectangle([62, 494, 79, 501], fill="black")
    mask_circle.rectangle([103, 494, 174, 501], fill="black")
    mask_circle.rectangle([181, 494, 227, 501], fill="black")
    mask_circle.rectangle([235, 494, 246, 501], fill="black") 
    circle_center = (MASK_SIZE[0] // 2, MASK_SIZE[1] // 2)
    mask_circle.ellipse((circle_center[0] - sun_circle_pixel_radius,
                        circle_center[1] - sun_circle_pixel_radius, 
                        circle_center[0] + sun_circle_pixel_radius, 
                        circle_center[1] + sun_circle_pixel_radius), 
                        fill=(0,0,0,255))
    return mask_image

def overlap_images(mask_image, overlapped_image):
    overlapped_image.convert("RGBA")
    position = (0, 0)
    overlapped_image.paste(mask_image, position, mask_image)

def first_filter_white_pixels_percentage(overlapped_image):
    low_threshold_binary_image = overlapped_image.convert("1")
    first_filter_pixels = low_threshold_binary_image.getdata()
    first_filter_white_pixels = sum(1 for pixel in first_filter_pixels if pixel == 255)
    first_filter_total_pixels = len(first_filter_pixels)
    return (first_filter_white_pixels/first_filter_total_pixels)*100 

def second_filter_white_pixels_percentage(overlapped_image):
    grey_image = overlapped_image.convert("L")
    high_threshold_binary_image = grey_image.point(lambda p: 255 if p > PIXEL_LIGHT_INTENSITY_THRESHOLD else 0, "1")
    second_filter_pixels = high_threshold_binary_image.getdata()
    second_filter_white_pixels = sum(1 for pixel in second_filter_pixels if pixel == 255)
    second_filter_total_pixels = len(second_filter_pixels)
    return (second_filter_white_pixels/second_filter_total_pixels)*100

def lambda_handler(event, context):
  for record in event['Records']:
    source_bucket = record['s3']['bucket']['name']
    destination_bucket = 'bucket-destino-erupcion-solar'
    key = unquote_plus(record['s3']['object']['key'])
    tmpkey = key.replace('/', '')
    base_name, extension = os.path.splitext(tmpkey)
    output_key = f"{base_name}_output{extension}"
    json_key = f"metadata-{base_name}.json"
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
    upload_image_path = '/tmp/processed-{}'.format(tmpkey)
    upload_json_path = '/tmp/metadata-{}.json'.format(tmpkey)
    s3_client.download_file(source_bucket, tmpkey, download_path)
    with Image.open(download_path) as img:
        img.save(upload_image_path)
    sun_circle_pixel_radius = circle_detection(download_path)
    mask_image = create_mask_image(sun_circle_pixel_radius)
    with Image.open(download_path) as overlapped_image:
        overlap_images(mask_image, overlapped_image)
        first_filter_percentage = first_filter_white_pixels_percentage(overlapped_image)
        if (first_filter_percentage >= FIRST_FILTER_WHITE_PIXELS_PERCENTAGE_THRESHOLD):
            s3_client.upload_file(upload_image_path, destination_bucket, output_key)
            detection = "filtro_1"
            white_pixels_percentage = first_filter_percentage
            metadata = {"detection": detection, "white_pixels_percentage": white_pixels_percentage}
            with open(upload_json_path, 'w') as json_file:
                json.dump(metadata, json_file)
            s3_client.upload_file(upload_json_path, destination_bucket, json_key)
        else:
            second_filter_percentage = second_filter_white_pixels_percentage(overlapped_image)
            if (second_filter_percentage >= SECOND_FILTER_WHITE_PIXELS_PERCENTAGE_THRESHOLD):
                s3_client.upload_file(upload_image_path, destination_bucket, output_key)
                detection = "filtro_2"
                white_pixels_percentage = second_filter_percentage
                intensity = "weak"
                if(white_pixels_percentage > SECOND_FILTER_STRONG_SOLAR_FLARE_THRESHOLD):
                    intensity = "strong"
                elif (white_pixels_percentage >= SECOND_FILTER_MEDIUM_SOLAR_FLARE_THRESHOLD):
                    intensity = "medium" 
                metadata = {"detection": detection, "white_pixels_percentage": white_pixels_percentage, "intensity": intensity}
                with open(upload_json_path, 'w') as json_file:
                    json.dump(metadata, json_file)
                s3_client.upload_file(upload_json_path, destination_bucket, json_key)