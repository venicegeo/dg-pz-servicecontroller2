# pz_servicecontroller
Repository containing the framework/implementation of the Piazza ServiceController.  The ServiceController controls the registration, management and execution of service instances.    

The ServiceController serves as a central location for Piazza users to register, discover and utilize services external to the Piazza core.  Service instances that are registered within Piazza expose a remote API (HTTP/REST) at a given location (URL containing host, port, servicename, etc.). A service instance is a web service which is external to the Piazza framework, but is registered within Piazza so it can be discovered and utilized by Piazza users.  

Service developers can develop and register services using the Piazza JSON API.   The ServiceController contains a REST service which handles client requests and controls the execution of services.   The ServiceController also listens to specific ServiceController topics to handle requets for service management.

For details on running and using the ServiceController, see https://github.com/venicegeo/venice/wiki/Pz-ServiceController



