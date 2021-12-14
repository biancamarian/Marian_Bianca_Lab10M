﻿using gRPCClient.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Marian_Bianca_Lab10;
using System.Threading;
using Grpc.Core;

namespace gRPCClient.Controllers
{
    
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
        
        public async Task<IActionResult> Unary(int? id)
        {
            var channel = GrpcChannel.ForAddress("https://localhost:5001");
            var client = new Greeter.GreeterClient(channel);
            if (id == null) { 
            var reply = await client.SendStatusAsync(new SRequest { No = 3 });
            return View("ShowStatus", (object)ChangetoDictionary(reply));
            }
            else
            {
                var reply = await client.SendStatusAsync(new SRequest { No = 3 }) ;
                if (id == 3) {
                reply = await client.SendStatusAsync(new SRequest { No = (int)id }); 
                }
                return View("ShowStatus", (object)ChangetoDictionary(reply));
            }
        }
        private Dictionary<string, string> ChangetoDictionary(SResponse response)
        {
            Dictionary<string, string> statusDict = new Dictionary<string, string>();
            foreach (StatusInfo status in response.StatusInfo)
                statusDict.Add(status.Author, status.Description);
            return statusDict;
        }
      
        public async Task<IActionResult> ServerStreaming(string id)
        {
            var channel = GrpcChannel.ForAddress("https://localhost:5001");
            var client = new Greeter.GreeterClient(channel);
            Dictionary<string, string> statusDict = new Dictionary<string, string>();
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(5));
            if (id == null)
            {
                using (var call = client.SendStatusSS(new SRequest { No = 6 }, cancellationToken: cts.Token))
                {
                    try
                    {
                        await foreach (var message in call.ResponseStream.ReadAllAsync())
                        {
                            statusDict.Add(message.StatusInfo[0].Author, message.StatusInfo[0].Description);
                        }
                    }
                    catch (RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.Cancelled)
                    {
                        // Log Stream cancelled
                    }
                }
                return View("ShowStatus", (object)statusDict);
            }
            else {
                    using (var call = client.SendStatusSS(new SRequest { No = 5 }, cancellationToken: cts.Token))
                    {
                        try
                        {
                            await foreach (var message in call.ResponseStream.ReadAllAsync())
                            {
                            if (id == message.StatusInfo[0].Author)
                            {
                                statusDict.Add(message.StatusInfo[0].Author, message.StatusInfo[0].Description);
                            }
                            }
                        }
                        catch (RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.Cancelled)
                        {
                            // Log Stream cancelled
                        }
                    } 
                return View("ShowStatus", (object)statusDict);

            }
        }

        public async Task<IActionResult> ClientStreaming(int [] s)
        {
            var channel = GrpcChannel.ForAddress("https://localhost:5001");
            var client = new Greeter.GreeterClient(channel);
            Dictionary<string, string> statusDict = new Dictionary<string, string>();
           // int[] statuses = { 3, 2, 4 };
          
                using (var call = client.SendStatusCS())
                {
                    foreach (var sT in s)
                    {
                       await call.RequestStream.WriteAsync(new SRequest { No = sT });
                    }
                    await call.RequestStream.CompleteAsync();
                    SResponse sRes = await call.ResponseAsync;
                    foreach (StatusInfo status in sRes.StatusInfo)
                        statusDict.Add(status.Author, status.Description);
                }
                return View("ShowStatus", (object)statusDict);
            
        }

        public async Task<IActionResult> BiDirectionalStreaming(int[] s)
        {
            var channel = GrpcChannel.ForAddress("https://localhost:5001");
            var client = new Greeter.GreeterClient(channel);
            Dictionary<string, string> statusDict = new Dictionary<string, string>();
          //  int[] statusNo = { 3, 2, 4 };
            
                using (var call = client.SendStatusBD())
                {
                    var responseReaderTask = Task.Run(async () =>
                    {
                        while (await call.ResponseStream.MoveNext())
                        {
                            var response = call.ResponseStream.Current;
                            foreach (StatusInfo status in response.StatusInfo)
                                statusDict.Add(status.Author, status.Description);
                        }
                    });

                    foreach (var sT in s)
                    {
                            await call.RequestStream.WriteAsync(new SRequest { No = sT });
                        
                    }
                    await call.RequestStream.CompleteAsync();
                    await responseReaderTask;
                }
            

            return View("ShowStatus", (object)statusDict);
        }

    }
}
