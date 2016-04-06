using System;
using System.Threading;

namespace Consumer
{
    abstract class ConsoleApp
    {
        private Thread _consoleThread;

        protected void ListenToConsole(
            Action quitCallback,
            Action pauseCallback = null,
            Action resumeCallback = null
            )
        {
            _consoleThread = new Thread(() =>
            {
                Console.CancelKeyPress += (sender, eventArgs) =>
                {
                    Console.WriteLine("Kill!");
                    _consoleThread.Abort();
                };
                while (true)
                {
                    var input = Console.ReadKey(true);
                    if (input.KeyChar == 'p' && pauseCallback != null)
                    {
                        pauseCallback();
                        Console.WriteLine("Paused.");
                    }
                    if (input.KeyChar == 'r' && resumeCallback != null)
                    {
                        resumeCallback();
                        Console.WriteLine("Resumed.");
                    }
                    if (input.KeyChar == 'q' && quitCallback != null)
                    {
                        Console.WriteLine("Shutting down...");
                        quitCallback();
                        break;
                    }
                }
            });
            _consoleThread.Start();
        }
    }
}
