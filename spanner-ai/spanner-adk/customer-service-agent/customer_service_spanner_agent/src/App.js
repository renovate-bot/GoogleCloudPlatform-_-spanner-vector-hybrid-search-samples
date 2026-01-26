/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, { useState, useEffect, useCallback } from 'react';
import { Send, Package, ShoppingCart, Info, RefreshCcw } from 'lucide-react'; // Using lucide-react for icons

function App() {
  const [userId, setUserId] = useState('user123'); // Default user ID for demonstration
  const [orderId, setOrderId] = useState('');
  const [productId, setProductId] = useState('');
  const [userQuery, setUserQuery] = useState('');
  const [chatHistory, setChatHistory] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isAuthReady, setIsAuthReady] = useState(true); // Assuming authentication is ready for this demo
  const [sessionId, setSessionId] = useState(() => `session_${crypto.randomUUID()}`); // Generate a unique session ID
  const [isSessionCreating, setIsSessionCreating] = useState(false); // Track if session is being created
  const [sessionCreated, setSessionCreated] = useState(false); // Track if session is successfully created

  // The name of your agent, as defined in agent.py (e.g., LlmAgent(name="customer_service_spanner_agent", ...))
  const appName = 'customer_service_spanner_agent'; // Must match the name in your agent.py
  // Removed backendBaseUrl as requests will now be proxied from the frontend's origin

  // Placeholder for an authentication token if your backend ADK agent's API requires it.
  const authToken = 'YOUR_AUTH_TOKEN_HERE'; // e.g., 'Bearer abc123def456' or an API key

  // Effect to scroll chat to bottom when new messages arrive
  useEffect(() => {
    const chatContainer = document.getElementById('chat-container');
    if (chatContainer) {
      chatContainer.scrollTop = chatContainer.scrollHeight;
    }
  }, [chatHistory]);

  // Function to create or update a session with the backend agent
  const createSession = useCallback(async () => {
    setIsSessionCreating(true);
    setSessionCreated(false); // Reset session created status before attempting to create
    // Endpoint for session creation is relative to the frontend's origin, which will be proxied
    const sessionEndpoint = `/apps/${appName}/users/${userId}/sessions/${sessionId}`;

    try {
      const response = await fetch(sessionEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          // Add auth token if needed for session creation endpoint
          // 'Authorization': `Bearer ${authToken}`,
        },
        body: JSON.stringify({
          "temp:current_user_id": userId,
        })
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Session creation failed! Status: ${response.status}, Details: ${errorText}`);
      }

      console.log('Session created/updated successfully:', await response.json());
      setSessionCreated(true);
      setChatHistory(prev => [...prev, { sender: 'system', text: 'Session with agent established.' }]);
    } catch (error) {
      console.error("Error creating/updating session:", error);
      setChatHistory(prev => [...prev, {
        sender: 'system',
        text: `Failed to initialize session. Please ensure the backend agent is running on http://localhost:8000 and your React app's package.json has "proxy": "http://localhost:8000". Error: ${error.message}`
      }]);
    } finally {
      setIsSessionCreating(false);
    }
  }, [appName, userId, sessionId]); // Dependencies for useCallback

  // Effect to create session on component mount or userId/sessionId change
  useEffect(() => {
    // Only attempt to create session if not already creating and not already created
    if (userId && !isSessionCreating && !sessionCreated) {
      createSession();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [userId, sessionId, sessionCreated]); // Dependencies for useEffect


  // Function to handle sending query to the backend agent and receiving response
  const handleAgentResponse = async (query) => {
    setIsLoading(true);

    // Ensure session is created before sending a query
    if (!sessionCreated) {
      setChatHistory(prev => [...prev, {
        sender: 'system',
        text: "Agent session is not yet established. Please wait for the session to initialize, or try resetting the app."
      }]);
      setIsLoading(false);
      return;
    }

    // Endpoint for running the agent is relative to the frontend's origin, which will be proxied
    const runEndpoint = `/run`;
    console.log('user query: ', query);

    // Construct the payload to send to the ADK agent, matching the /run endpoint's documented structure
    const payload = {
      app_name: appName,
      user_id: userId,
      session_id: sessionId, // Session ID is always sent with each run request
      new_message: {
        role: "user",
        parts: [
          { text: query } // The query already contains embedded context IDs
        ]
      }
    };

    let agentMessage = {
      sender: 'agent',
      text: "I'm having trouble processing your request. Please try again later, or check backend agent logs."
    };

    try {
      // Make the API call to the ADK agent backend
      const response = await fetch(runEndpoint, { // Use the relative path
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          // --- AUTHENTICATION HEADER (Add if your agent API requires it) ---
          // 'Authorization': `Bearer ${authToken}`,
          // -----------------------------------------------------------------
        },
        body: JSON.stringify(payload)
      });

      // Check if the response was successful
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP error! Status: ${response.status}, Details: ${errorText}`);
      }

      const result = await response.json();

      // The /run endpoint's response structure is a list of events.
      // The final agent response is typically found in the 'text' field of a 'part'
      // within the 'content' of the first relevant event where the role is 'model'.
      if (result && Array.isArray(result) && result.length > 0) {
        // Find the first event that has a text response from the model
        const textResponseEvent = result.find(event =>
          event.content?.role === 'model' &&
          event.content?.parts?.[0]?.text
        );

        if (textResponseEvent) {
          agentMessage.text = textResponseEvent.content.parts[0].text;
        } else {
          // Fallback if structured output isn't found, try to stringify the whole result
          agentMessage.text = "Agent responded, but couldn't parse output. Raw response: " + JSON.stringify(result);
        }
      } else {
        agentMessage.text = "The agent returned an empty response. Please check the backend logs.";
      }
    } catch (error) {
      console.error("Error communicating with agent backend:", error);
      // Update error message to reflect proxying
      agentMessage.text = `I couldn't reach the agent service via proxy. Ensure the backend agent is running on http://localhost:8000 and your React app's package.json has "proxy": "http://localhost:8000". Error: ${error.message}`;
    } finally {
      setIsLoading(false);
      setChatHistory(prev => [...prev, agentMessage]);
    }
  };

  // Handles form submission (user sending a message)
  const handleSubmit = (e) => {
    e.preventDefault();
    if (!userQuery.trim()) return;

    const userMessage = { sender: 'user', text: userQuery };
    setChatHistory(prev => [...prev, userMessage]);
    // Embedding the context into the query string for the agent to parse.
    // The agent's instruction in agent.py should be designed to extract these.
    // IMPORTANT: This is the message sent to the agent!!!!!
    // Security issue: The user query can hack to ask for other user's data.
    const fullQuery = `Customer ID: ${userId}, Order ID: ${orderId}, Product ID: ${productId}. My query: ${userQuery}`;
    handleAgentResponse(fullQuery);
    setUserQuery('');
  };

  // Resets the chat and input fields, generates a new session ID, and then re-creates the session
  const handleReset = () => {
    setOrderId('');
    setProductId('');
    setUserQuery('');
    setChatHistory([]);
    setIsLoading(false);
    setSessionId(`session_${crypto.randomUUID()}`); // Generate a new session ID for a fresh start
    setSessionCreated(false); // Mark session as not created
    // The useEffect will trigger createSession due to sessionId change
  };

  const isInputDisabled = isLoading || isSessionCreating || !sessionCreated || !isAuthReady; // Inputs disabled until session is ready

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-100 to-indigo-200 flex items-center justify-center p-4 font-inter">
      <div className="bg-white rounded-2xl shadow-xl w-full max-w-4xl flex flex-col md:flex-row overflow-hidden h-[700px]">
        {/* Left Panel - Input and Context */}
        <div className="w-full md:w-1/3 p-6 bg-blue-50 text-blue-900 flex flex-col justify-between overflow-y-auto">
          <div>
            <h2 className="text-2xl font-bold mb-6 flex items-center gap-2">
              <ShoppingCart className="text-blue-600" size={28} /> E-commerce Agent
            </h2>
            <div className="mb-4">
              <label htmlFor="userId" className="block text-sm font-medium text-blue-700 mb-1">Customer ID (Sent as `user_id` & embedded in query)</label>
              <input
                id="userId"
                type="text"
                value={userId}
                onChange={(e) => setUserId(e.target.value)}
                className="w-full p-2 border border-blue-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
              />
            </div>
            <div className="mb-4">
              <label htmlFor="sessionId" className="block text-sm font-medium text-blue-700 mb-1">Session ID (Sent as `session_id`)</label>
              <input
                id="sessionId"
                type="text"
                value={sessionId}
                className="w-full p-2 border border-blue-300 rounded-lg bg-gray-100 cursor-not-allowed"
                disabled
              />
               {isSessionCreating && <p className="text-sm text-blue-600 mt-1">Initializing session...</p>}
               {!isSessionCreating && !sessionCreated && <p className="text-sm text-red-500 mt-1">Session not established.</p>}
               {!isSessionCreating && sessionCreated && <p className="text-sm text-green-600 mt-1">Session established.</p>}
            </div>
            <div className="mb-4">
              <label htmlFor="orderId" className="block text-sm font-medium text-blue-700 mb-1">Order ID (Embedded in Query)</label>
              <input
                id="orderId"
                type="text"
                value={orderId}
                onChange={(e) => setOrderId(e.target.value)}
                placeholder="e.g., ORD001"
                className="w-full p-2 border border-blue-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
              />
            </div>
            <div className="mb-4">
              <label htmlFor="productId" className="block text-sm font-medium text-blue-700 mb-1">Product ID (Embedded in Query)</label>
              <input
                id="productId"
                type="text"
                value={productId}
                onChange={(e) => setProductId(e.target.value)}
                placeholder="e.g., PROD101"
                className="w-full p-2 border border-blue-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
              />
            </div>
            <p className="text-sm text-blue-600 mt-2">
              **Note:** Order, and Product IDs are embedded directly into your chat query for the agent.
              Ensure your Spanner instance has relevant data for the agent to retrieve meaningful responses.
            </p>
          </div>
          <button
            onClick={handleReset}
            className="w-full mt-6 bg-gray-200 text-gray-700 py-2 px-4 rounded-lg hover:bg-gray-300 transition-all flex items-center justify-center gap-2 shadow-sm"
          >
            <RefreshCcw size={18} /> Reset App
          </button>
        </div>

        {/* Right Panel - Chat Interface */}
        <div className="w-full md:w-2/3 p-6 flex flex-col bg-white">
          <div id="chat-container" className="flex-grow overflow-y-auto pr-2 mb-4 space-y-4">
            {chatHistory.length === 0 ? (
              <div className="text-center text-gray-500 py-10">
                <Info className="mx-auto mb-2 text-gray-400" size={36} />
                <p>Start a conversation with the agent!</p>
                <p className="text-sm">Ask about order status, product details, or returns.</p>
                {isSessionCreating && <p className="text-blue-600 mt-2">Initializing session with agent...</p>}
                {!isSessionCreating && !sessionCreated && <p className="text-red-500 mt-2">Error: Session not established. Ensure backend is running.</p>}
              </div>
            ) : (
              chatHistory.map((message, index) => (
                <div
                  key={index}
                  className={`flex ${message.sender === 'user' ? 'justify-end' : 'justify-start'}`}
                >
                  <div
                    className={`max-w-[75%] p-3 rounded-lg shadow-md ${ 
                      message.sender === 'user'
                        ? 'bg-blue-500 text-white'
                        : 'bg-gray-100 text-gray-800'
                    }`}
                  >
                    {message.text.split('\n').map((line, i) => (
                      <p key={i} className="whitespace-pre-wrap">{line}</p>
                    ))}
                  </div>
                </div>
              ))
            )}
            {isLoading && (
              <div className="flex justify-start">
                <div className="max-w-[75%] p-3 rounded-lg shadow-md bg-gray-100 text-gray-800">
                  <div className="flex items-center space-x-2">
                    <div className="w-3 h-3 bg-gray-400 rounded-full animate-bounce-slow" style={{ animationDelay: '0s' }}></div>
                    <div className="w-3 h-3 bg-gray-400 rounded-full animate-bounce-slow" style={{ animationDelay: '0.2s' }}></div>
                    <div className="w-3 h-3 bg-gray-400 rounded-full animate-bounce-slow" style={{ animationDelay: '0.4s' }}></div>
                  </div>
                </div>
              </div>
            )}
          </div>

          <form onSubmit={handleSubmit} className="flex gap-2">
            <input
              type="text"
              value={userQuery}
              onChange={(e) => setUserQuery(e.target.value)}
              placeholder="Ask about your order, a product, or returns..."
              className="flex-grow p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all shadow-sm"
              disabled={isInputDisabled}
            />
            <button
              type="submit"
              className="bg-blue-600 text-white p-3 rounded-lg hover:bg-blue-700 transition-all flex items-center justify-center shadow-md disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={isInputDisabled || !userQuery.trim()}
            >
              <Send size={20} />
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}

// Tailwind CSS animation for loading dots
const style = document.createElement('style');
style.innerHTML = `
@keyframes bounce-slow {
  0%, 100% { transform: translateY(0); }
  50% { transform: translateY(-5px); }
}
.animate-bounce-slow {
  animation: bounce-slow 1s infinite;
}
`;
document.head.appendChild(style);

export default App;
