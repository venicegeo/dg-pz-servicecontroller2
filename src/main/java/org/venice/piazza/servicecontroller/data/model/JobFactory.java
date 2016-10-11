/*******************************************************************************
 * Copyright 2016, RadiantBlue Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.venice.piazza.servicecontroller.data.model;


import model.job.Job;
import model.request.PiazzaJobRequest;

import org.joda.time.DateTime;

/**
 * Wraps PiazzaJobRequests and PiazzaJobTypes into Job objects, which contain
 * metadata about that Job. The Job objects are ultimately what are stored in
 * the Job Manager's database.
 * 
 * From pz-jobmanager TODO need to discuss about moving to pz-jobcommon
 * 
 */
public class JobFactory {
	public static Job fromJobRequest(PiazzaJobRequest jobRequest, String jobId) {
		Job job = new Job();
		job.setJobId(jobId);
		job.jobType = jobRequest.jobType;
		job.createdBy = jobRequest.createdBy;
		job.status = "Submitted"; // TODO: Enum
		job.createdOn = new DateTime();
		return job;
	}
}