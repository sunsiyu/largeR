##########################################################################
# DEMONSTRATION CODE FOR R MEETUP JULY 27, 2010
# Taking R to the Limit: Part 1, Parallelization
# by Ryan Rosario
#
# Code originates from sources cited in the last slide of my presentation.
#
# If this file is retransmitted or shared, please distribute with this
# header intact.
###########################################################################


#####################
# MPI
# Demonstration 1
#####################
library(Rmpi)                                   
# Spawn as many slaves as possible
mpi.spawn.Rslaves()             
# Cleanup if R quits unexpectedly
.Last <- function(){
  if (is.loaded("mpi_initialize")){
    if (mpi.comm.size(1) > 0){
      print("Please use mpi.close.Rslaves() to close slaves.")
      mpi.close.Rslaves()
    }
    print("Please use mpi.quit() to quit R")
    .Call("mpi_finalize")
  }
}

# Tell all slaves to return a message identifying themselves
mpi.remote.exec(paste("I am",mpi.comm.rank(),"of",mpi.comm.size()))

# Tell all slaves to close down, and exit the program
mpi.close.Rslaves()
mpi.quit()


#####################
# MPI (Fibonacci)
# Code Walkthrough
#Task pull
#####################

if  (!is.loaded("mpi_initialize")) {
  library("Rmpi")
}

# Spawn as many slaves as possible
mpi.spawn.Rslaves()
# Cleanup if R quits unexpectedly
.Last <- function(){
  if (is.loaded("mpi_initialize")){
    if (mpi.comm.size(1) > 0){
      print("Please use mpi.close.Rslaves() to close slaves.")
      mpi.close.Rslaves()
    }
    print("Please use mpi.quit() to quit R")
    .Call("mpi_finalize")
  }
}


fib <- function(n)
{
  if (n < 1)
    stop("Input must be an integer >= 1")
  if (n == 1 | n == 2)
    1
  else
    fib(n-1) + fib(n-2)
}

slavefunction <- function() {
  # Note the use of the tag for sent messages:  
  #     1=ready_for_task, 2=done_task, 3=exiting
  # Note the use of the tag for received messages:  
  #     1=task, 2=done_tasks
  junk <- 0
  
  done <- 0
  while (done != 1) {
    # Signal being ready to receive a new task
    mpi.send.Robj(junk,0,1)
    
    # Receive a task
    task <- mpi.recv.Robj(mpi.any.source(),mpi.any.tag())
    task_info <- mpi.get.sourcetag()
    tag <- task_info[2]
    
    if (tag == 1) {
      # perform the task, and create results
      
      #TASK: Compute the Fibonacci number
      n <- task$n
      val <- ifelse(n == 1 | n == 2, 1, fib(n-1) + fib(n-2))
      print(val)
      res <- list(result=val, n=n)
      
      # Send the results back as a task_done message
      mpi.send.Robj(res,0,2)
    }
    else if (tag == 2) {
      # Master is saying all tasks are done.  Exit
      done <- 1
    }
    # Else ignore the message or report an error
  }
  
  # Tell master that this slave is exiting.  Send master an exiting message
  mpi.send.Robj(junk,0,3)
}


#Send the function to the slaves.
mpi.bcast.Robj2slave(slavefunction)
mpi.bcast.Robj2slave(fib)
#Call the function in all the slaves to get them ready
#to take on work.
mpi.bcast.cmd(slavefunction())

# Create list of tasks and store in 'tasks' 
tasks <- vector('list')
for (i in 1:25)
{
  tasks[[i]] <- list(n=i)
}


junk <- 0
closed_slaves <- 0
n_slaves <- mpi.comm.size()-1

results <- rep(0,25)

while (closed_slaves < n_slaves) {
  # Receive a message from a slave
  message <- mpi.recv.Robj(mpi.any.source(),mpi.any.tag())
  message_info <- mpi.get.sourcetag()
  slave_id <- message_info[1]
  tag      <- message_info[2]
  
  if (tag == 1) {
    # slave is ready for a task.  Give it the next task, or tell it tasks
    # are done if there are none.
    if (length(tasks) > 0) {
      # Send a task, and then remove it from the task list
      mpi.send.Robj(tasks[[1]], slave_id, 1);
      tasks[[1]] <- NULL
    }
    else {
      mpi.send.Robj(junk, slave_id, 2)
    }
  }
  else if (tag == 2) {
    # The message contains results.  Do something with the results.
    # You might store them in a data structure, or do some other processing
    # You might even create more tasks and add them to the end of the
    # tasks list if you like in order to have these sub-tasks done by the
    # slaves
    res <- message$result
    results[message$n] <- res
  }
  else if (tag == 3) {
    # A slave has closed down. 
    closed_slaves <- closed_slaves + 1
  }
}

# Operate on and/or display your results
# ...

# close slaves and exit.
mpi.close.Rslaves()
mpi.quit()


#####################
# snowfall / sockets
# Demonstration 3
#####################

#Load necessary libraries
library(snowfall)

#Initialize
sfInit(parallel=TRUE, cpus=2, type="SOCK", socketHosts=rep("localhost",2))
#parallel=FALSE runs code in sequential mode.
#cpus=n, the number of CPUs to use.
#type can be "SOCK" for socket cluster, "MPI", "PVM" or "NWS"
#socketHosts is used to specify hostnames/IPs for 
#remote socket hosts in "SOCK" mode only.

#Load the data.
require(mvna)
data(sir.adm)
#using a canned dataset.

#create a wrapper that can be called by a list function.
wrapper <- function(idx) {	
  #I could pass a parameter, but unnecessary
  index <- sample(1:nrow(sir.adm), replace=TRUE)
  temp <- sir.adm[index, ]
  fit <- crr(temp$time, temp$status, temp$pneu)
  return(fit$coef)
}

#export needed data to the workers and load packages on them.
sfExport("sir.adm")	#export the data (multiple objects in a list)
sfLibrary(cmprsk)	#force load the package (must be installed)

#STEP 5: start the network random number generator
sfClusterSetupRNG()
#STEP 6: distribute the calculation
result <- sfLapply(1:1000, wrapper)
#return value of sfLapply is always a list!

#STEP 7: stop snowfall
sfStop()


#######################
# parallel and collect
# Demonstration 4
#######################

library(multicore)

my.silly.loop <- function(j, duration) {
  i <- 0
  while (i < duration) {
    i <- i + 1
  }
  #When done, return TRUE to indicate that this function does *something*
  return(paste("Silly", j, "Done"))
}
silly.1 <- parallel(my.silly.loop(1, 10000000))
silly.2 <- parallel(my.silly.loop(2, 5000000))
collect(list(silly.1, silly.2))
#Example of blocking.


#######################
# parallel and collect
# Demonstration 5
#######################

silly.1 <- parallel(my.silly.loop(1, 10000000))
silly.2 <- parallel(my.silly.loop(2, 5000000))
collect(list(silly.1, silly.2), wait=FALSE, timeout=2)
#we can get the results later by calling,
collect(list(silly.1, silly.2))
#Example of pre-emption



#######################
# parallel and collect
# Demonstration 6
#######################

status <- function(results.so.far) {
  jobs.completed <- sum(unlist(lapply(results.so.far, FUN=function(x) { !is.null(x) })))
  print(paste(jobs.completed, "jobs completed so far."))
}
silly.1 <- parallel(my.silly.loop(1, 10000000))
silly.2 <- parallel(my.silly.loop(2, 5000000))
results <- collect(list(silly.1, silly.2), intermediate=status)


#######################
# foreach
# Demonstration 7
#######################

library(foreach)
library(doMC)

registerDoMC()

data(iris)

iris.sub <- iris[which(iris[, 5] != "setosa"), c(1,5)]
trials <- 10000
result <- foreach(icount(trials), .combine=cbind) %dopar% {
  indices <- sample(100, 100, replace=TRUE)
  glm.result <- glm(iris.sub[indices, 2]~iris.sub[indices, 1], family=binomial("logit"))
  coefficients(glm.result)   #this is the result!
}