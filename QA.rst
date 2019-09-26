Questions
---------

* What assumptions are we making about how data flows into and out of the system?
  * We need to meet with other teams (like phenotyping) to see how the expect our data output to integrate with their systems
  * Data storage format below is probably a big part of this
* What is the processing model? Is this going to be batch? streaming?
* Do we integrate distributed workflow tools like spark?
* What is our underlying storage model for this data so that other pieces of the pipeline and applications can use it? Same model for export to VA?
* How does document classification fit into this?
* Haven’t talked about text search at all yet
  * What is our requirement here because VA has been pretty vague on what they expect on that
  * Do we have to develop a UI for them to use?
* Need to meet with VA sponsors regarding AIM 6: suicide-relevant concepts. We need definitions and term list from them and to sort out how we’re going to integrate that into our current IE pipeline.
