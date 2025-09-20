import React from 'react'
import ReactDOM from 'react-dom/client'
import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import 'bootstrap/dist/css/bootstrap.min.css'
import './theme.css'
import App from './pages/App'
import Catalogue from './pages/Catalogue'
import GraphPage from './pages/GraphPage'
import StatsPage from './pages/StatsPage'
import SettingsPage from './pages/SettingsPage'
import IngestPage from './pages/IngestPage'
import LoginPage from './pages/LoginPage'
import ProfilePage from './pages/ProfilePage'
import UsersPage from './pages/UsersPage'
import AdminTasksPage from './pages/AdminTasksPage'
import AdminLogsPage from './pages/AdminLogsPage'
import AdminLlmPage from './pages/AdminLlmPage'
import AdminCollectionsPage from './pages/AdminCollectionsPage'
import ToastProvider from './ui/Toasts'
import ActionLogProvider from './ui/ActionLog'
import AuthProvider from './ui/Auth'

const router = createBrowserRouter([
  {
    path: '/',
    element: <App />,
    children: [
      { index: true, element: <Catalogue /> },
      { path: 'graph', element: <GraphPage /> },
      { path: 'stats', element: <StatsPage /> },
      { path: 'settings', element: <SettingsPage /> },
      { path: 'ingest', element: <IngestPage /> },
      { path: 'profile', element: <ProfilePage /> },
      { path: 'users', element: <UsersPage /> },
      { path: 'admin/tasks', element: <AdminTasksPage /> },
      { path: 'admin/logs', element: <AdminLogsPage /> },
      { path: 'admin/llm', element: <AdminLlmPage /> },
      { path: 'admin/collections', element: <AdminCollectionsPage /> },
    ],
  },
  { path: '/login', element: <LoginPage /> },
], { basename: '/app' })

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ActionLogProvider>
      <ToastProvider>
        <AuthProvider>
          <RouterProvider router={router} />
        </AuthProvider>
      </ToastProvider>
    </ActionLogProvider>
  </React.StrictMode>
)
